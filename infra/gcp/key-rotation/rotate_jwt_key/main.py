# Cloud Function: oj-rotate-jwt-key
#
# Triggered monthly by the `oj-key-rotation` Cloud Scheduler job. Produces a
# new HS256 signing key, writes a new Secret Manager version of `oj-jwt-keys`
# carrying:
#
#   {
#     "current":  "v<N+1>",        # new key id — signs all NEW tokens
#     "previous": ["v<N>"],        # one-key overlap window — accepted on verify
#     "keys": {                    # full key map the gateway must accept
#       "v<N>":   "<old-secret>",  # carried over so tokens minted under v<N>
#                                  # still validate during the overlap
#       "v<N+1>": "<new-secret>",  # the new key
#     }
#   }
#
# Older Secret Manager versions are retained (default versioning) so an
# operator can roll back to a previous N-pair by `gcloud secrets versions
# disable LATEST`. The api-gateway always reads `versions/latest`.
#
# After the 7-day overlap, the NEXT monthly run will sweep v<N>: it reads the
# current latest, sees `current=v<N+1>, previous=[v<N>]`, and rotates to
# `current=v<N+2>, previous=[v<N+1>]` — dropping v<N> from the key map.
#
# Sweep timing: monthly cadence ≫ 7-day overlap ≫ 15-min access TTL. So by
# the time the function is invoked for month K+1, every access token minted
# in month K under v<N> has long-since expired. The "7-day overlap" is the
# WORST-case window during which the operator can roll back without ejecting
# active sessions; the steady-state overlap is the full inter-rotation gap
# (~1 month).
#
# Idempotency: re-invoking within the same calendar minute mints another
# secret version. That's acceptable — only `versions/latest` is read. Cloud
# Scheduler retries with exponential backoff up to 3 attempts.
#
# Configuration via env vars on the Cloud Function:
#   SECRET_ID            — defaults to "oj-jwt-keys"
#   PROJECT_ID           — GCP project id (required)
#   KEY_LENGTH_BYTES     — defaults to 48 (random_password length in
#                          terraform's tfstate-held v1 key)

import base64
import json
import os
import secrets
import sys
from typing import Optional

# google-cloud-secret-manager is part of the Cloud Functions Python runtime;
# no extra packaging needed.
from google.cloud import secretmanager  # type: ignore


def _new_secret(length_bytes: int) -> str:
    """Return a URL-safe random secret of `length_bytes` bytes of entropy.

    Output is base64-urlsafe-no-pad which gives length_bytes ≈ 4/3 * length_bytes
    characters — comfortably above HS256's 32-byte minimum for any sensible
    length_bytes >= 24.
    """
    raw = secrets.token_bytes(length_bytes)
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _read_latest(client: secretmanager.SecretManagerServiceClient,
                 project_id: str, secret_id: str) -> Optional[dict]:
    """Return the parsed latest version, or None if no version exists yet."""
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        resp = client.access_secret_version(request={"name": name})
    except Exception as e:  # NotFound on first run is expected
        print(f"[rotate-jwt-key] no existing latest version ({e}); bootstrapping fresh")
        return None
    return json.loads(resp.payload.data.decode("utf-8"))


def _kid_after(prev: Optional[dict]) -> tuple[str, str]:
    """Return (current_kid, previous_kid) for the new version."""
    if prev is None:
        # First rotation ever — terraform tfstate holds v1, so the function
        # introduces v2 and lists v1 as the overlap kid.
        return ("v2", "v1")
    cur = prev.get("current", "v1")
    if not cur.startswith("v"):
        raise ValueError(f"unexpected kid format: {cur}")
    try:
        n = int(cur[1:])
    except ValueError as e:
        raise ValueError(f"unexpected kid format: {cur}") from e
    return (f"v{n+1}", cur)


def rotate(request):  # noqa: ARG001 — Cloud Functions HTTP entry point
    project_id = os.environ["PROJECT_ID"]
    secret_id = os.environ.get("SECRET_ID", "oj-jwt-keys")
    key_length = int(os.environ.get("KEY_LENGTH_BYTES", "48"))

    client = secretmanager.SecretManagerServiceClient()
    prev = _read_latest(client, project_id, secret_id)
    new_kid, prev_kid = _kid_after(prev)
    new_secret_value = _new_secret(key_length)

    # Carry the previous-kid's key forward so tokens still in flight verify.
    # Drop anything OLDER than the previous kid — the design doc says we keep
    # at most ONE overlap key in the map. Operators who need to extend the
    # overlap edit the secret payload by hand and disable this function for
    # the cycle.
    new_keys: dict[str, str] = {}
    if prev is not None:
        prev_keys = prev.get("keys", {})
        if prev_kid in prev_keys:
            new_keys[prev_kid] = prev_keys[prev_kid]
        else:
            # Defensive: terraform tfstate holds v1 even after the function
            # has rotated forward, so v1's key bytes are NOT recoverable from
            # tfstate here. Pull it from the previous version of THIS secret
            # if available. If it isn't (first rotation), this is the v1 path
            # and prev_kid is "v1" which we can't read from tfstate either.
            # In that case the function ALSO doesn't need the v1 key bytes —
            # the api-gateway already has v1 from JWT_SECRET inline config.
            print(f"[rotate-jwt-key] previous kid {prev_kid} not in prev secret payload; "
                  "skipping carry-forward — inline JWT_SECRET seed remains valid")
    new_keys[new_kid] = new_secret_value

    payload = {
        "current": new_kid,
        "previous": [prev_kid] if prev_kid else [],
        "keys": new_keys,
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    parent = f"projects/{project_id}/secrets/{secret_id}"
    resp = client.add_secret_version(
        request={"parent": parent, "payload": {"data": body}}
    )
    print(f"[rotate-jwt-key] new version: {resp.name}  kid={new_kid}  previous={prev_kid}")
    return {"version": resp.name, "current": new_kid, "previous": prev_kid}, 200


if __name__ == "__main__":
    # Local dry-run hook: `PROJECT_ID=foo python3 main.py` exercises the same
    # code path as the Cloud Function, useful for one-shot manual rotations.
    sys.exit(rotate(None))
