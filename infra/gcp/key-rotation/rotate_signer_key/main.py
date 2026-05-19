# Cloud Function: oj-rotate-signer-key
#
# Triggered monthly (offset by ~14 days from the JWT rotation, so we don't
# stack two key churns on the same night). Mints a fresh JSON key for the
# `oj-problem-signer` service account and writes it as a new version of the
# `oj-problem-signer-key` Secret Manager secret. Then triggers a
# problem-service container bounce on each region VM.
#
# Why a simpler design than JWT:
#   - GCS V4-signed URLs have a 5-minute TTL. After 5 minutes from the last
#     sign with the old key, NO URLs in flight reference the old key — so the
#     "overlap window" we need is the URL TTL, not weeks.
#   - The problem-service holds the credentials object in JVM memory; we
#     can't hot-swap that cleanly without a refactor (Google's
#     ServiceAccountCredentials is intentionally immutable). So instead we
#     bounce the container — the worker retries the test-case GET on 5xx,
#     the bounce is sub-second, contestant impact is invisible.
#
# Sequence:
#   1. Mint new SA key (`iam.serviceAccountKeys.create`).
#   2. Write payload (the JSON key file bytes) as a new Secret Manager
#      version.
#   3. Disable previous Secret Manager version after a 6-minute grace
#      window (one URL TTL + 1 min slack) — actually we DON'T disable it
#      from the function; we ask the systemd timer + container to flip,
#      then leave the old version DISABLED via a separate scheduled job
#      `oj-key-rotation-sweep-signer` that runs the next day.
#   4. (Out-of-band, done by the VM-side systemd timer `oj-signer-key.timer`)
#      Refetch the secret; bounce the problem-service container.
#
# This function deliberately does NOT delete the old SA key; key deletion is
# a one-way operation and we keep two generations live so that a botched
# rotation can be rolled back by re-enabling the previous Secret Manager
# version. Old SA keys are reaped by a separate weekly tidy-up
# (`gcloud iam service-accounts keys delete`) once they're >30 days old.
#
# Configuration via env vars:
#   PROJECT_ID                — GCP project id (required)
#   SIGNER_SA_EMAIL           — full SA email (required)
#   SECRET_ID                 — defaults to "oj-problem-signer-key"

import os
import sys

from google.cloud import iam_credentials_v1  # type: ignore  # noqa: F401 (placeholder)
from google.cloud import secretmanager  # type: ignore
from googleapiclient.discovery import build  # type: ignore


def rotate(request):  # noqa: ARG001 — Cloud Functions HTTP entry point
    project_id = os.environ["PROJECT_ID"]
    signer_email = os.environ["SIGNER_SA_EMAIL"]
    secret_id = os.environ.get("SECRET_ID", "oj-problem-signer-key")

    iam = build("iam", "v1")
    name = f"projects/{project_id}/serviceAccounts/{signer_email}"
    resp = iam.projects().serviceAccounts().keys().create(
        name=name,
        body={
            "privateKeyType": "TYPE_GOOGLE_CREDENTIALS_FILE",
            "keyAlgorithm": "KEY_ALG_RSA_2048",
        },
    ).execute()

    # `privateKeyData` is base64-encoded; the Secret Manager value we want is
    # the raw JSON file bytes (what the application expects on disk).
    import base64
    private_key_json = base64.b64decode(resp["privateKeyData"])

    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}/secrets/{secret_id}"
    version = client.add_secret_version(
        request={"parent": parent, "payload": {"data": private_key_json}}
    )
    print(f"[rotate-signer-key] new version: {version.name}")
    return {"version": version.name, "sa_key": resp["name"]}, 200


if __name__ == "__main__":
    sys.exit(rotate(None))
