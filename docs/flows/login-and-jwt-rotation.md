# Login and JWT Rotation

> Last reconciled with the repo on 2026-05-19.
>
> How a contestant authenticates once and stays logged in for hours of contest time without ever exposing a long-lived bearer token to JavaScript.

## 1. Why this flow exists

A contest can last 5 hours. A short-lived JWT (15 min) would force the SPA to re-prompt for credentials mid-contest — unacceptable. A long-lived JWT (5 h) is a juicy theft target. The split is: short-lived access JWT in memory + opaque refresh token in an HttpOnly cookie, rotated on every refresh, with token-family revocation when reuse is detected. The full design is in [`design-docs/auth-end-to-end.md`](../design-docs/auth-end-to-end.md); this flow is the runtime view.

## 2. Sequence

```mermaid
sequenceDiagram
    autonumber
    participant SPA
    participant GW as api-gateway
    participant CRDB as CockroachDB
    SPA->>GW: POST /api/v1/auth/signup {email, handle, password}
    GW->>CRDB: INSERT users (password_hash=Argon2id(password))
    SPA->>GW: POST /api/v1/auth/login {email, password}
    GW->>CRDB: SELECT password_hash FROM users
    Note over GW: Argon2id.verify(password, hash)
    GW->>CRDB: INSERT refresh_tokens (family_id, sha256(raw))
    GW-->>SPA: {access_jwt} + Set-Cookie: refresh=<raw>; HttpOnly; SameSite=Strict
    SPA->>GW: POST /api/v1/submissions (Authorization: Bearer access_jwt)
    Note over GW: access JWT signature + claims validated
    SPA->>GW: POST /api/v1/auth/refresh (cookie: refresh=<raw>)
    GW->>CRDB: SELECT WHERE family_id=? AND token_hash=sha256(raw)
    alt token found and not yet rotated
        GW->>CRDB: UPDATE refresh_tokens SET rotated_at=now() WHERE id=old
        GW->>CRDB: INSERT refresh_tokens (family_id=same, sha256(new))
        GW-->>SPA: {new access_jwt} + Set-Cookie: refresh=<new>; HttpOnly
    else token found AND rotated_at IS NOT NULL (replay)
        GW->>CRDB: UPDATE refresh_tokens SET revoked_at=now() WHERE family_id=?
        GW-->>SPA: 401 Unauthorized — re-authenticate
    end
```

## 3. Step-by-step walkthrough

1. **Signup.** `api-gateway/src/main/java/com/onlinejudge/gateway/controller/AuthController.java#signup` accepts `{email, handle, password}`. The password is hashed via Argon2id (`Argon2PasswordEncoder` in `SecurityConfig.java`) with memory cost ~64 MB and iteration cost 3 — chosen to take ~150 ms on the api-gateway VM, slow enough to defeat offline brute-force, fast enough not to amplify a slow-loris attack.
   *Invariant:* the plaintext password never reaches storage. The hash is per-user (no shared salt across the table).

2. **Login.** `AuthController#login` looks up the user by email, calls `Argon2PasswordEncoder.matches(password, storedHash)`. On match, mints two tokens via `JwtTokenProvider`:
   - **Access JWT**: 15-min expiry, HMAC-SHA256 signed with the active signing key (key id rotated daily; old keys retained 7 days for in-flight validation).
   - **Refresh token**: 32 random bytes from `SecureRandom`, sent in an `HttpOnly; Secure; SameSite=Strict` cookie. Stored server-side as SHA-256 hash with a `family_id` for the rotation chain.

3. **Authenticated request.** The SPA puts the access JWT in `Authorization: Bearer ...`. `JwtAuthenticationFilter` (Spring Security) parses the JWT, validates signature against the active key set, checks `exp` claim. No DB lookup on the hot path — the JWT is self-contained.
   *Invariant:* a valid access JWT signature ⇒ the request acts on behalf of the user named in the `sub` claim, up to the JWT's `exp`.

4. **Refresh.** When the access JWT is within ~2 min of expiry, the SPA calls `POST /api/v1/auth/refresh` (cookie auto-sent by the browser). `AuthController#refresh` reads the cookie, hashes it, and looks up the row by `(family_id, token_hash)`. Three branches:
   - **Token valid and unrotated** → mark the old row `rotated_at=now()`, insert a new row with the same `family_id` and a fresh raw token, return new access JWT + Set-Cookie.
   - **Token valid but already rotated** (replay attack — somebody else stole the refresh token and used it first) → revoke the entire family (UPDATE all rows with this `family_id` to set `revoked_at`). Return 401. The legitimate user is forced to re-authenticate.
   - **Token not found** → 401.

5. **Logout.** `POST /api/v1/auth/logout` revokes the current `family_id`, clears the cookie.

## 4. Failure modes at each step

| Step | Failure | Detection | Behaviour |
|---|---|---|---|
| 1 | Email already exists | UNIQUE constraint violation | 409 Conflict |
| 2 | Argon2 verify mismatch | `matches()` returns false | 401; log with structured tag for rate limiter; per-`/auth/*` limiter applies (isolated from submission limits) |
| 2 | Brute-force burst | `/auth/*` rate limiter on IP + email | 429 with `Retry-After` |
| 3 | Expired access JWT | `exp` claim check | 401; SPA quietly fires `/refresh` |
| 3 | Tampered access JWT | HMAC verify fail | 401; logged for monitoring |
| 3 | Signing key rotated, JWT signed by old key still in retention window | tried-keys list | accept; warn at p95 of usage |
| 4 | Refresh-token reuse detected | `rotated_at IS NOT NULL` on lookup | family revoked; security event emitted; contestant logs back in |
| 4 | Refresh token from a revoked family | `revoked_at IS NOT NULL` | 401 |
| 4 | Stolen cookie used from different `User-Agent` | (best-effort) UA mismatch heuristic | log; do NOT auto-revoke (false positives common) |

## 5. Related material

- Full design rationale: [`design-docs/auth-end-to-end.md`](../design-docs/auth-end-to-end.md).
- api-gateway owner page: [`services/api-gateway.md`](../services/api-gateway.md) §3.1 (auth implementation).
- Why Argon2id over bcrypt: hashing cost parameters chosen for the OJ workload.
- Why refresh tokens are NOT JWTs: rotation requires the server to know which token was used last; that's a stateful concept incompatible with the self-contained JWT design.
