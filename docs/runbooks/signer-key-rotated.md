# Runbook — Signer key rotated; problem-service URLs failing

> Operator playbook for "the worker can no longer fetch test cases because the GCS V4 signature is invalid — the signer key on disk has drifted from what GCP IAM trusts." Companion docs: [`../tech-spec.md#73-v4-signed-gcs-urls`](../tech-spec.md#73-v4-signed-gcs-urls), [`../services/problem-service.md`](../services/problem-service.md) (V4 signing internals; runbook §8.2), [`../tech-spec.md#74-secrets`](../tech-spec.md#74-secrets).

The problem-service signs V4 GCS URLs in-process with the `oj-problem-signer@…` SA's private key, loaded from `/var/secrets/gcs-signer.json` (bind-mounted from `/opt/oj/gcs-signer.json` on the host, mode `0400`). The host fetches the key from Secret Manager on every VM boot (`infra/gcp/startup/region.sh.tpl` step 7). When the key is rotated in Secret Manager but the new version doesn't make it to disk, the worker starts seeing **HTTP 403** (and in some rotation modes **HTTP 401**) on every signed URL.

Note on the alert wording: the tech-spec §11.1 table calls this case "problem-service 401-ing" / worker `test-case GET HTTP 401`. In practice the worker throws `IOException: test-case GET HTTP <code>` from `TestCaseFetcher.fetchOne()` (`execution-worker/src/main/java/com/onlinejudge/worker/service/TestCaseFetcher.java:157`) — the actual code GCS returns for a bad V4 signature is usually **403**, but a revoked SA can give **401**. This runbook covers both.

---

## 1. When to run this

- Worker logs: `IOException: test-case GET HTTP 401: https://storage.googleapis.com/oj-test-cases-<project>/...` (or `HTTP 403`).
- Every submission lands as `INTERNAL_ERROR` (mapped from the `IOException` per [`../services/execution-worker.md#5-failure-modes`](../services/execution-worker.md#5-failure-modes)).
- A signed URL pasted into `curl` from your laptop returns 403 with body containing `SignatureDoesNotMatch` or 401 with `InvalidCredentials`.
- problem-service itself returns 200 with a URL bundle — i.e. signing succeeds, fetching fails. This is the "key is stale" shape, distinct from problem-service not starting up at all.

If problem-service is returning 500 on `/api/v1/problems/.../test-cases` or its `/actuator/health` is DOWN, you have a key-load problem, not a key-rotation problem — see [`../services/problem-service.md#81-worker-cant-reach-problem-service`](../services/problem-service.md#81-worker-cant-reach-problem-service).

---

## 2. Detection

### 2.1 Confirm the key file is present and non-empty

```sh
# On the region's VM (control plane in single-region, oj-region-* in multi-region)
ls -la /opt/oj/gcs-signer.json
# Expected: -r-------- 1 root root <NNN> ... gcs-signer.json
# NNN typically 2300-2500 bytes for a service-account JSON.

# Quick non-empty + JSON-shape check
sudo jq -r 'keys[]' /opt/oj/gcs-signer.json 2>&1 | head
# Expected keys: type, project_id, private_key_id, private_key,
#   client_email, client_id, auth_uri, token_uri, ...
```

If the file is missing, zero bytes, or not valid JSON, the rest of the runbook is moot — jump to §4.1 and re-fetch from Secret Manager.

### 2.2 Confirm the worker is seeing the failure

```sh
# Errored test-case fetches in the last 200 lines
sudo docker logs oj-execution-worker --tail 200 2>&1 \
  | grep -E 'test-case GET HTTP' | tail -10

# Count by status code
sudo docker logs oj-execution-worker --tail 500 2>&1 \
  | grep -oE 'test-case GET HTTP [0-9]+' | sort | uniq -c
```

A repeated `HTTP 401` or `HTTP 403` for the same `oj-test-cases-<project>` host = the signature GCS receives doesn't match a trusted key.

### 2.3 Did problem-service load the key the worker is now signing against?

```sh
# When did the problem-service container start, vs when was the key mtime?
docker inspect oj-problem-service --format '{{.State.StartedAt}}'
stat -c '%y %n' /opt/oj/gcs-signer.json
```

If the file's mtime is **newer** than the container's StartedAt, problem-service is still signing with the OLD key it loaded at boot — that's the immediate fix (§4.2).

### 2.4 Quick black-box check

```sh
# Pull a fresh URL through problem-service and try to fetch it
URL=$(sudo docker exec oj-problem-service curl -s \
  "http://localhost:8089/api/v1/problems/00000000-0000-0000-0000-0000000cafee/test-cases?pretestOnly=true" \
  | jq -r '.test_cases[0].input_url')

curl -sI "$URL" | head -1
# 200 OK  → signer key on disk + in process is fine; problem is elsewhere
# 403     → signature does not match; key drift
# 401     → SA credentials revoked at GCP side, not just rotated
# 404     → GCS object missing; runbook docs/services/problem-service.md#83
```

---

## 3. Diagnose

### 3.1 What does Secret Manager have?

```sh
# Latest version metadata
gcloud secrets versions describe latest --secret=oj-problem-signer-key

# Compare against what's on disk (private_key_id is the SA-side identifier)
sudo jq -r '.private_key_id' /opt/oj/gcs-signer.json
gcloud secrets versions access latest --secret=oj-problem-signer-key | jq -r '.private_key_id'
```

If the two `private_key_id` values differ, the on-disk key is stale relative to Secret Manager. The startup script writes whichever Secret Manager version was `latest` at VM boot — if Secret Manager was bumped while the VM was running, the disk doesn't auto-sync.

### 3.2 Is the SA still authorized?

```sh
# Confirm IAM still grants the signer SA the necessary role
gcloud storage buckets get-iam-policy gs://oj-test-cases-<project> \
  | jq '.bindings[] | select(.role=="roles/storage.objectViewer") | .members'
# Should include 'serviceAccount:oj-problem-signer@<project>.iam.gserviceaccount.com'

# List the SA's currently-valid keys
gcloud iam service-accounts keys list \
  --iam-account=oj-problem-signer@<project>.iam.gserviceaccount.com
```

If the SA has zero active keys, signing produces bytes but no GCP-side key matches → HTTP 401 on fetch.

If the SA is missing the `objectViewer` binding, every signed URL is correctly signed but the access decision rejects it → HTTP 403.

### 3.3 Has Secret Manager been bumped?

```sh
gcloud secrets versions list oj-problem-signer-key --limit=5
```

If a new ENABLED version appeared in the last few hours and the older one is DISABLED/DESTROYED, the rotation just happened. This is the most common cause.

---

## 4. Mitigate

### 4.1 Key file missing or empty — refetch

```sh
sudo gcloud secrets versions access latest \
  --secret=oj-problem-signer-key \
  > /opt/oj/gcs-signer.json
sudo chmod 0400 /opt/oj/gcs-signer.json
sudo chown root:root /opt/oj/gcs-signer.json

# Sanity-check
sudo jq -r '.client_email,.private_key_id' /opt/oj/gcs-signer.json
```

The compose volume bind-mounts `/opt/oj/gcs-signer.json` into `/var/secrets/gcs-signer.json` (ro) — so the file is now visible to the container without restarting it. **But** the JVM has already parsed the key at boot; you still need §4.2.

### 4.2 problem-service is signing with a stale key — bounce it

```sh
sudo docker compose -f /opt/oj/region.yml restart oj-problem-service

# Watch for the readiness ping
until sudo docker exec oj-problem-service curl -sf http://localhost:8089/actuator/health >/dev/null; do
  echo "[wait] problem-service not ready"; sleep 3
done
echo "[ok] problem-service back"
```

The startup log will print `GcsConfig: loaded signer credentials from /var/secrets/gcs-signer.json` (or equivalent). If instead you see `GcsConfig: signer-credentials-path empty; falling back to ADC`, the env var didn't pick up the file — confirm `GCS_SIGNER_KEY_PATH=/var/secrets/gcs-signer.json` is in `region.yml`'s `oj-problem-service.environment` block.

### 4.3 Verify end-to-end

```sh
# Pull a fresh URL post-restart and curl it
URL=$(sudo docker exec oj-problem-service curl -s \
  "http://localhost:8089/api/v1/problems/00000000-0000-0000-0000-0000000cafee/test-cases?pretestOnly=true" \
  | jq -r '.test_cases[0].input_url')

curl -sI "$URL" | head -1
# Want: HTTP/2 200
```

Then re-run a stuck submission:

```sh
# Watch fresh worker logs for the IOException class to stop appearing
sudo docker logs --follow --tail 50 oj-execution-worker 2>&1 \
  | grep -E 'test-case GET|Verdict submission'
```

Within a few seconds the next submission cycle should clear without `test-case GET HTTP 4xx`.

### 4.4 SA permission revoked (HTTP 401 not 403)

If §3.2 showed the SA has no active keys OR the `objectViewer` binding is missing, the fix is at the IAM layer, not on disk. Reapply via terraform:

```sh
cd infra/gcp/terraform
GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)" \
  tofu apply -target=google_storage_bucket_iam_member.signer_object_viewer
```

If a key was deleted out of band, `tofu taint` the SA key and re-apply:

```sh
GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)" \
  tofu taint google_service_account_key.problem_signer
GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)" \
  tofu apply
```

The new key is now in Secret Manager (the terraform `random_id` + `google_secret_manager_secret_version` chain pushes it). Then run §4.1 + §4.2 on every VM.

### 4.5 Drain submissions stuck on the old key

While the key was bad, every submission emitted `INTERNAL_ERROR`. Those rows are in `idempotency_keys` with `status='completed'` (terminal). To reprocess them, operators can re-publish to `submissions.pretest` — see [`../services/execution-worker.md#85-dlq-topic-filling-up`](../services/execution-worker.md#85-dlq-topic-filling-up) for the DLQ-replay pattern; for non-DLQ rows the same `DlqReplay` tool can re-emit by `submissionId`.

---

## 5. Rollback / if mitigation went wrong

### 5.1 Replaced the key file but signing still fails

Most likely the JSON is malformed (e.g. `gcloud secrets versions access` output got piped through something that ate newlines). Re-fetch cleanly:

```sh
sudo gcloud secrets versions access latest \
  --secret=oj-problem-signer-key \
  --format='get(payload.data)' \
  | base64 -d \
  > /opt/oj/gcs-signer.json.new
sudo jq . /opt/oj/gcs-signer.json.new >/dev/null && \
  sudo mv /opt/oj/gcs-signer.json.new /opt/oj/gcs-signer.json
sudo chmod 0400 /opt/oj/gcs-signer.json
sudo docker compose -f /opt/oj/region.yml restart oj-problem-service
```

If `jq` fails on the `.new` file, the secret payload itself is corrupted in Secret Manager — escalate.

### 5.2 Rolled back to the previous secret version, still 403

GCP propagation of an IAM change is usually under a minute but can take up to 7 minutes. If you reverted to a known-good version (say, `--version=3` instead of `latest`) and curls still 403, give it 5 minutes. If still failing, the issue is not just rotation — check §3.2 IAM bindings.

### 5.3 Container loops on startup after key swap

If problem-service crash-loops post-restart with `FileNotFoundException: /var/secrets/gcs-signer.json` despite the file being present at `/opt/oj/gcs-signer.json`, the bind-mount is missing. Confirm the `volumes:` block under `oj-problem-service` in `region.yml`:

```yaml
volumes:
  - /opt/oj/gcs-signer.json:/var/secrets/gcs-signer.json:ro
```

Restore if missing, then `up -d` again.

### 5.4 Worker still emitting 401/403 after problem-service is fixed

The worker holds no cached URLs — every request re-fetches the bundle from problem-service. So if the worker still 4xx's, problem-service is signing wrong (verify §4.3 with curl) OR the bucket name in URLs is wrong (problem-service's `app.gcs.bucket` env var). Check:

```sh
sudo docker exec oj-problem-service printenv | grep -E 'GCS_|APP_GCS_'
```

If the bucket name in URLs doesn't match the actual bucket, the SA grant doesn't apply — looks like a rotation problem but is actually a config-drift problem.

---

## 6. Related incidents

- [`../services/problem-service.md#82-all-signed-urls-return-403-from-gcs`](../services/problem-service.md#82-all-signed-urls-return-403-from-gcs) — the in-service entry; this runbook is the cross-cutting version that walks through both 401 and 403.
- [`../services/execution-worker.md#84-all-submissions-stuck-on-processing`](../services/execution-worker.md#84-all-submissions-stuck-on-processing) — when the failure happens BEFORE the worker has claimed idempotency, rows don't get stuck. When it happens AFTER (rare race), see that section.
- [`../tech-spec.md#74-secrets`](../tech-spec.md#74-secrets) — the broader story: signer key + JWT secret both come from Secret Manager via the startup script.
- [`./multi-region.md`](./multi-region.md) — multi-region adds a wrinkle: each VM independently fetches the key on boot. If you rotate while one region is up and the other down, restarting the down one re-fetches automatically (good); the up one stays stale until you bounce problem-service there.

---

## 7. Escalation

- If `gcloud secrets versions access latest` itself errors with `PermissionDenied`, the host VM's SA (`oj-control-plane@…`) has lost `roles/secretmanager.secretAccessor`. That's a terraform drift / IAM revocation issue — escalate, and as a stopgap `gcloud secrets add-iam-policy-binding` the role back.
- If the SA's private-key-id on disk does NOT match anything in `gcloud iam service-accounts keys list`, the key was deleted at GCP side without rotating the on-disk copy. This is the "key rotation cron missing" debt called out in [`../tech-spec.md#14-known-limitations-and-debt`](../tech-spec.md#14-known-limitations-and-debt) — escalate so someone schedules the rotation; manual unblock is §4.4 (tf taint).
- Repeated post-rotation 403s with `SignatureDoesNotMatch` on URLs that look fine: this can indicate clock drift on the VM. `timedatectl` should show `NTP service: active` and `System clock synchronized: yes`. If the VM clock is more than 5 minutes off, the V4 signature TTL doesn't intersect the GCS-side clock and every URL is "expired before issued".
- Multi-region note: after rotating the key, **bounce problem-service in every region**. The Secret Manager fetch happens at VM boot; an already-running VM keeps the old key in JVM memory indefinitely. If you only restart one region's problem-service, the other region will keep emitting INTERNAL_ERROR — and the gateway's region-mismatch 307 will route some traffic over to it.
