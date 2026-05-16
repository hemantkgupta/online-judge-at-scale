-- =============================================================================
-- V5: Real authentication — passwords + refresh tokens + auth event log.
--
-- Replaces the dev-token-mint endpoint with real signup/login/refresh/logout.
-- Roadmap section 2.1.
--
-- Assumes Agent S has already unified the api-gateway datasource onto the
-- `onlinejudge` database (V3 created the users / problems / test_cases tables
-- there). Everything below is `IF NOT EXISTS` so re-running is a no-op.
--
-- Notes
-- -----
--  * password_hash is added with a DEFAULT '' so existing rows (created via
--    V3's bare-username insert path) don't fail NOT NULL. Operators must
--    backfill real Argon2id hashes for those rows before a follow-up V6
--    drops the default. Rows with password_hash='' should be rejected by
--    AuthService at login time (no valid Argon2 hash starts with empty).
--  * created_at is added in case V3 ran on a database where the column was
--    missing — V3 itself does include it, so this is defence-in-depth.
--  * refresh_tokens stores SHA-256 of the raw token, never the token itself.
--    The raw token is returned to the client once at issuance time and never
--    persisted server-side. Even a full DB dump cannot impersonate users.
--  * auth_events.user_id is NULLABLE so we can record failed signups (no user
--    row yet) and login-with-unknown-username attempts.
-- =============================================================================

ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash STRING NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS refresh_tokens (
    token_hash STRING    PRIMARY KEY,        -- SHA-256(raw refresh token), hex-encoded
    user_id    UUID      NOT NULL REFERENCES users(id),
    expires_at TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP,                    -- NULL while active
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_refresh_user ON refresh_tokens (user_id, expires_at);

CREATE TABLE IF NOT EXISTS auth_events (
    id          UUID      PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID,                        -- nullable: failed pre-user events
    event_type  STRING    NOT NULL,          -- SIGNUP, LOGIN_OK, LOGIN_FAIL, REFRESH, LOGOUT
    ip_address  STRING,
    user_agent  STRING,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    detail      STRING                       -- free-form, e.g. "invalid password"
);

CREATE INDEX IF NOT EXISTS idx_auth_events_user_time
    ON auth_events (user_id, created_at);
