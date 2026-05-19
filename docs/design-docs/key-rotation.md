# Key Rotation: JWT Signing Secret + GCS V4 Signer SA Key

*Design document for [`tech-spec.md` §14](../tech-spec.md#14-known-limitations-and-debt) item M2 ("JWT secret + signer SA key both in tfstate, no rotation cron"), implementing roadmap §3.3 and §3.4.*

## Problem

Two secrets in the deployment live in `terraform.tfstate` today, with no rotation discipline:

1. **`random_password.jwt_secret`** in `infra/gcp/terraform/main.tf` — the HS256 signing key for every access token the api-gateway mints. Every `tofu destroy && apply` silently invalidates every token in circulation. A `tofu state pull` is enough to forge any user's session. There's no kid versioning in the deployed system, no path to introduce a new key without ejecting active contestants, and no scheduled rotation cron.

2. **`google_service_account_key.problem_service_signer_key`** — the JSON private key for `oj-problem-signer@…`, which signs the V4 GCS download URLs the execution-worker uses to pull test-case bytes. Lives in tfstate identical to the JWT secret, with the same compromise blast radius (read tfstate → mint URLs that read the test-cases bucket for the next 5 minutes per URL). No rotation cron either; today's "fix" is `tofu taint && apply`, which is a re-mint without overlap.

These two compromises have the same answer: **versioned keys + dual-key validation + a periodic rotation job**. This doc covers both, with a slightly different orchestration story for each because their consumption patterns differ (JWT keys live in JVM memory and can be hot-swapped via an atomic snapshot; the GCS signer credentials are intentionally immutable in the Java SDK and need a container bounce).

The system cannot be considered production-ready until a compromised secret can be rotated out of the trust set within minutes without a service restart for contestants and within a single cycle for ops. None of those properties exists today.

## Design

### Key versioning scheme

Every JWT carries a `kid` (key id) JWS header. The header names which member of the gateway's accepted key set was used to sign. Verification looks the key up by `kid`; if the kid is unknown the request fails fast with a clear `JwtException("Token signed with unknown kid='…'")` — not a vague "bad signature" — so rotation triage in logs is unambiguous.

Key ids are `v<N>` for monotonically increasing `N`. The initial deploy carries `v1` (seeded from the `random_password.jwt_secret` tfstate resource); the first rotation introduces `v2`, the next `v3`, and so on.

At any moment the gateway holds a **set** of keys, of which one is current:

```
{ current: "vN+1",
  keys:    { "vN":   <bytes>,  // the overlap key — verify only
             "vN+1": <bytes> } // the current key — signs new tokens
}
```

New tokens are always minted under `current`. Inbound tokens whose `kid` matches any member of `keys` (including the overlap one) verify. After the overlap window (default 7 days), the next rotation drops `vN` from the map.

The same shape generalises to longer overlaps — operators who anticipate a contest weekend and want to delay the sweep can keep TWO overlap kids in the payload and skip one rotation cycle.

### Secret Manager layout

One Secret Manager secret per concern:

| Secret id | Owner-writer | Reader | Payload |
|---|---|---|---|
| `oj-jwt-keys` | `oj-key-rotator` SA (Cloud Function `oj-rotate-jwt-key`) | `oj-region` SA on each VM | JSON `{ "current": "vN", "previous": ["vN-1"], "keys": { "vN-1": "...", "vN": "..." } }` |
| `oj-problem-signer-key` | `oj-key-rotator` SA (Cloud Function `oj-rotate-signer-key`) | `oj-region` SA on each VM | Raw service-account JSON key bytes (a single key, no versioning in the payload — Secret Manager's own versioning is the only ledger) |

Secret Manager keeps every version. Rollback is one click in the console (`disable LATEST`); the api-gateway picks up the now-effective previous version on its next poll.

### In-process key store (api-gateway)

`api-gateway/src/main/java/com/onlinejudge/gateway/security/JwtKeyStore.java` is the new abstraction. It holds an `AtomicReference<Snapshot>` where each `Snapshot` is `(currentKid, Map<kid, SecretKey>)`. `JwtTokenProvider` reads exactly one snapshot per request — sign and verify happen against the same map even if a reload races them.

Two configuration modes:

1. **Inline keys** (default; local dev + every unit test): keys come from the existing `app.jwt.keys.<kid>` Spring properties. No polling. Identical wire to the pre-rotation deployment.

2. **Sidecar file** (production): set `app.jwt.key-store.path=/var/secrets/jwt-keys.json`. The file is the JSON payload of the Secret Manager secret, mirrored onto the VM by the `oj-refresh-secrets.timer` systemd timer every 60 s. JwtKeyStore polls the path every `app.jwt.key-store.poll-interval-ms` (default 60 s); the worst-case staleness is `2 × 60 s = 2 min` — fine for a 7-day overlap window.

Reload semantics:

- File unchanged (byte-hash match) → no-op.
- File parses → atomic `snapshot.set(...)`. In-flight requests using the old snapshot continue with the old map; the next request picks up the new map.
- File corrupt / unreadable → log a `WARN`, keep the previous snapshot. A polling failure must never take the gateway down.

### Rotation flow (JWT)

1. **Cloud Scheduler** (`oj-key-rotation`, default schedule `0 2 1 * * UTC`, retries 3x with exponential backoff) POSTs to **Cloud Function** `oj-rotate-jwt-key`.
2. The function reads `versions/latest` of `oj-jwt-keys`. Extracts the current kid `vN`. Decides the new kid `vN+1`.
3. Generates a fresh 48-byte URL-safe-base64 secret.
4. Composes the new payload: `current=vN+1`, `previous=[vN]`, `keys={ vN: <carried-forward>, vN+1: <new> }`.
5. `add_secret_version` writes the new latest. Old versions remain (for forensic + rollback).
6. On each region VM, `oj-refresh-secrets.timer` (every 60 s) reads `versions/latest` and atomically swaps `/opt/oj/jwt-keys.json` if the bytes changed.
7. Inside the api-gateway container, `JwtKeyStore` polls `/var/secrets/jwt-keys.json` every 60 s, detects the changed hash, parses, and `set`s the new snapshot.
8. New tokens are minted under `vN+1`. Tokens previously minted under `vN` continue to validate until the next rotation drops `vN`.

End-to-end propagation: **Cloud Function complete → all gateways serving new tokens ≤ 2 minutes** (one Secret Manager mirror + one in-process poll). This is the SLO-load-bearing number for an emergency rotation; the monthly cadence has weeks of slack.

### Rotation flow (GCS V4 signer SA key)

Simpler because the overlap window we need is *the URL TTL*, not weeks. V4-signed URLs expire 5 minutes after they were signed; the LAST URL signed with the old key is gone from the wire after 5 minutes.

1. **Cloud Scheduler** (`oj-key-rotation-signer`, default `0 2 15 * * UTC` — offset 14 days from the JWT job so we never stack two key churns) POSTs to **Cloud Function** `oj-rotate-signer-key`.
2. The function calls `iam.serviceAccountKeys.create` on `oj-problem-signer@…` to mint a fresh JSON key.
3. `add_secret_version` writes the new key bytes to `oj-problem-signer-key`.
4. On each region VM, `oj-refresh-secrets.timer` notices the changed bytes, atomically replaces `/opt/oj/gcs-signer.json`, and runs `docker restart oj-problem-service`. The bounce is sub-second; the worker retries the test-case GET on 5xx so in-flight submissions see at most one retry's worth of latency.
5. The OLD key is still ACTIVE on the GCP side until the operator deletes it. URLs signed with it remain valid until their 5-minute TTL elapses. After 6 minutes, no in-flight URL references the old key and it can be safely deleted (a weekly tidy-up cron — out of scope here — reaps SA keys older than 30 days).

Why a restart and not a hot swap: Google's `ServiceAccountCredentials` is intentionally immutable once constructed. A hot-swap would need a `GcsSigner` refactor to hold an `AtomicReference<ServiceAccountCredentials>` and rebuild on file-mtime change. Doable, but **the signed-URL contract already gives us a 5-minute overlap for free** — restarting is the simpler answer with no contestant-visible impact. The trade-off is documented in [`docs/services/problem-service.md`](../services/problem-service.md#33-signer-key-loading).

### Overlap timeline

The 7-day overlap window for JWT keys is the operator's emergency rollback budget, not the steady-state window. The monthly cadence gives ~30 days of overlap by default — the "7 days" is the MINIMUM we commit to ops:

```
month K:   v1 signs all new tokens. Secret payload: { current: "v1" }.
day 1:     Cloud Function adds v2.
             Payload:  { current: "v2", previous: ["v1"], keys: { v1, v2 } }
             Effect:   New tokens carry kid=v2. Tokens minted before
                       02:00 UTC today carry kid=v1; they verify until
                       their 15-minute access TTL expires.
day 1+1m:  All gateway pods have picked up the new payload (≤ 2 min).
           Last v1-signed token expires at day 1 + 17 minutes (15 min TTL + 2 min refresh).
day 8:     v1 has been a verification-only kid for 7 days. If an operator
           wants to roll BACK to v1, they `gcloud secrets versions
           disable LATEST` — the gateway picks up the previous payload
           within 2 minutes and signs again as v1.
day 30:    Next month's rotation runs. Reads current=v2, mints v3,
           writes { current: "v3", previous: ["v2"], keys: { v2, v3 } }.
           v1 is gone from the map. Any token still in circulation that
           carries kid=v1 fails fast.
```

The 7-day overlap is the SLA-critical knob. Shorter → rollback budget too tight. Longer → bigger blast radius if v1 leaked. 7 days survives a contestant weekend and lines up with on-call rotations.

### Rollback

- **Bad new key (unparseable, wrong size, etc.)**. `JwtKeyStore.reloadFromSidecar` throws; the polling-quiet path logs a `WARN` and the previous snapshot stays. Effect: contestants unaffected, ops notified by the WARN-rate alert.
- **Key compromise — known**. `gcloud secrets versions disable <version>` on the compromised version, then re-run the rotation function manually (`gcloud functions call oj-rotate-jwt-key`). End-to-end recovery: ~3 minutes (one Function invocation + one Secret Manager mirror cycle + one in-process poll).
- **Botched rotation rollback**. `gcloud secrets versions disable LATEST` reverts to the previous payload. The gateway picks it up within 2 minutes. Tokens minted in the brief intervening window become invalid — small blast radius given the access TTL is 15 min.
- **Initial v1 bootstrap from tfstate**. The first ever rotation reads "no previous version exists" and seeds v2 onto whatever v1 the inline `JWT_SECRET` env var carried. Subsequent rotations are self-contained. After two cycles the operator can `tofu state rm random_password.jwt_secret` to remove the last tfstate dependency — out of scope for this commit.

### SLO impact

| Metric | Pre-rotation | With rotation |
|---|---|---|
| `oj.gateway.auth.validate.success_rate` | Unaffected by rotation (no rotation exists) | Unaffected during a healthy rotation. During the 2-minute propagation gap, tokens minted under `vN+1` by a pod that has reloaded fail validation at a pod that hasn't — until the second pod's next poll. Worst-case auth failure rate during this window: `#stale-pods / #total-pods` of all new-token traffic, for ≤ 2 min. Single-region today → 0%. Multi-region two-pod → up to 50%. Acceptable for a once-a-month event; mitigated by staggering pod restarts in roadmap §3.7. |
| `oj.gateway.auth.validate.latency_p99` | n/a | Unaffected. Snapshot read is an `AtomicReference.get()`, sub-microsecond. |
| `oj.problem.sign.latency_p99` | ~80 ms | Unaffected. Signer credentials live in JVM memory; the only rotation event is a container bounce (~ 1 s startup) which the existing worker retry budget already absorbs. |
| `oj.problem.test_case_fetch.success_rate` | Unaffected by rotation | Brief ~ 1 s window where the bounced problem-service returns connection-refused. The worker `ack.nack(5s)` retry path already covers this — net effect on contestant-visible verdict success is zero. |

### Observability

Three log lines tagged for alerting:

- `[jwt-key-store] reloaded sidecar — current kid=<kid>, kids=<set>` — emitted on every successful reload that changes the map. Used as a heartbeat: alert if NO such line appears for `> 35 days` (rotation cron silently broken).
- `[jwt-key-store] poll reload failed; keeping previous snapshot. error=<…>` — emitted on every failed reload. Alert on `> 5 occurrences in 10 min` (Secret Manager IAM lapse or sidecar file going missing).
- `[rotate-jwt-key] new version: <name>` — emitted by the Cloud Function. Surfaces in Cloud Logging; alert on `< 1 success in 35 days` (cron schedule misconfigured).

A future metric `oj_gateway_jwt_keys_active{kid="vN"}` (gauge per kid in the snapshot) lets dashboards visualise the overlap window directly.

## Implementation phases

1. **Phase A — In-process key store (this commit).** Extract `JwtKeyStore` from `JwtTokenProvider`; add the polling sidecar reader. Spring-side: the inline keys still work unchanged. Unit tests cover dual-key validation, unknown-kid failure, sidecar parse, atomic snapshot swap, corrupt-file tolerance.
2. **Phase B — Cloud Function + Cloud Scheduler (this commit).** `infra/gcp/key-rotation/rotate_jwt_key/` and `rotate_signer_key/`. Terraform wiring in `infra/gcp/terraform/key-rotation.tf`. Two monthly schedules offset by 14 days.
3. **Phase C — VM-side mirror (this commit).** `oj-refresh-secrets.timer` writes the Secret Manager `versions/latest` to disk every 60 s. The api-gateway picks it up via JwtKeyStore; the problem-service is bounced when the signer file changes.
4. **Phase D — Tfstate cleanup (deferred).** Once two rotations have run successfully, the operator can drop `random_password.jwt_secret` and the inline `JWT_SECRET` env var. Tracked separately; not in this commit because the cleanup is irreversible.
5. **Phase E — Multi-pod stagger (deferred, roadmap §3.7).** Once api-gateway runs more than one pod per region, restart staggering ensures the 2-minute propagation window doesn't bring two pods out of sync at the same moment. For today's single-pod-per-region topology this is moot.

## Risks

- **Cloud Function regression.** A bug in `rotate_jwt_key` that writes a malformed payload silently breaks every gateway. Mitigation: JwtKeyStore tolerates parse failures (keeps previous snapshot) AND the function's payload is validated by the function itself before `add_secret_version`. Defense in depth — the gateway is the second line.
- **Secret Manager outage.** The `oj-refresh-secrets.timer` fails; the on-disk file stays stale. JwtKeyStore continues with the last-known-good snapshot. Worst-case: rotation is delayed by however long Secret Manager is down. Tokens minted during the outage are still valid; verification succeeds.
- **Clock skew across gateways.** Rotation invalidates a key by removing it from the map, not by an `exp` field — so clock skew across pods is irrelevant. JWT `exp` itself is at-most 15 minutes; standard NTP keeps the gateway clocks within seconds.
- **Two regions, different reload timing.** Pod A reloads at T=T0; pod B at T=T0+90s. Between those points, a request that lands on B with a kid=vN+1 token (signed by A) fails to verify. This is the 2-minute multi-pod window the SLO table calls out. Mitigation: poll interval is the load-bearing knob; tighten if needed.
- **Lost rollback budget.** If a key is leaked AND the operator can't re-disable Secret Manager versions in time, the overlap window means the leaked key keeps signing for 7 days. Mitigation: emergency rotation runs the Cloud Function manually + immediately disables the leaked version's payload, shrinking the effective overlap to < 5 minutes (one Secret Manager mirror cycle).

## Acceptance criteria

- `./gradlew :api-gateway:test` passes — including the new `JwtKeyStoreRotationTest` dual-key suite.
- `tofu plan` from `infra/gcp/terraform/` shows: new Secret Manager secret, two Cloud Functions, two Cloud Scheduler jobs, the rotator SA + IAM bindings, the source-archive bucket. No diff to existing VM resources except metadata-only changes in `startup-script` (the systemd-timer install commands).
- A manual `gcloud functions call oj-rotate-jwt-key` produces a new Secret Manager version of `oj-jwt-keys` with a fresh kid.
- After ~ 90 s, `curl http://<gateway>:8088/actuator/info` (or a log scrape) shows the new `current kid` from `[jwt-key-store] reloaded sidecar` log.
- A token minted before the rotation continues to verify until the next rotation cycle drops its kid.

## Related

- [`tech-spec.md` §7.1](../tech-spec.md#71-jwt--signuplogin) — JWT contract on the wire.
- [`tech-spec.md` §7.3](../tech-spec.md#73-v4-signed-gcs-urls) — V4-signed URL story.
- [`auth-end-to-end.md`](./auth-end-to-end.md) — the parent auth design; this doc is the rotation chapter that was deferred there.
- [`docs/services/problem-service.md` §3.3](../services/problem-service.md#33-signer-key-loading) — signer-key loading from the consumer side; updated to call out the bounce-on-rotate trade-off.
