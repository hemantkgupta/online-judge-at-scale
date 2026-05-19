package com.onlinejudge.gateway.security;

import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the map of {@code kid → SecretKey} that {@link JwtTokenProvider} uses
 * to sign and validate JWTs, and (optionally) hot-reloads it from a JSON
 * sidecar file on disk.
 *
 * <p>This is the in-process half of the JWT key-rotation story described in
 * {@code docs/design-docs/key-rotation.md}. The full chain:
 *
 * <pre>
 *   Cloud Scheduler (oj-key-rotation, monthly @ 02:00 UTC on the 1st)
 *      └─ Cloud Function "oj-rotate-jwt-key"
 *           └─ writes a NEW Secret Manager version of "oj-jwt-keys"
 *                payload = { "current": "v3", "previous": "v2",
 *                            "keys": { "v2": "...", "v3": "..." } }
 *           (older versions still readable for forensic / rollback)
 *      └─ on each region VM, a 60 s systemd timer (`oj-jwt-keys.timer`)
 *           runs `gcloud secrets versions access latest --secret=oj-jwt-keys`
 *           and writes the JSON atomically to /opt/oj/jwt-keys.json
 *           (mounted into the api-gateway container at /var/secrets/jwt-keys.json)
 *      └─ this class polls that file every {@code app.jwt.key-store.poll-interval-ms}
 *           and swaps the in-memory map atomically when the file's content hash
 *           changes — no restart needed, no in-flight tokens broken (kid lookup
 *           handles the overlap).
 * </pre>
 *
 * <p>Two configuration modes:
 * <ul>
 *   <li><b>Inline keys</b> (default; matches the pre-rotation deployment + every
 *       test): keys come from {@code app.jwt.keys.<kid>} properties, the current
 *       kid from {@code app.jwt.kid-current}, optional previous from
 *       {@code app.jwt.kid-previous}. No polling. Same wire as before the
 *       refactor — back-compat is the whole point.</li>
 *   <li><b>Sidecar file</b> (production rotation): set
 *       {@code app.jwt.key-store.path} to the JSON file. The schema is:
 *       <pre>
 *         {
 *           "current":  "v3",
 *           "previous": ["v2"],
 *           "keys":     { "v2": "...", "v3": "..." }
 *         }
 *       </pre>
 *       {@code previous} is an array so the operator can keep MORE than one
 *       overlap key during a contest weekend (i.e. delay the sweep). All keys
 *       in the {@code keys} map are accepted on verification; only
 *       {@code current} signs new tokens.</li>
 * </ul>
 *
 * <p>If both are configured the sidecar file wins; the inline keys serve as
 * the seed map until the first successful poll. This lets the api-gateway boot
 * even if the file is briefly unavailable.
 *
 * <p>Thread-safety: the snapshot is held in an {@link AtomicReference} so the
 * verifier path (called on every authenticated request) sees a consistent
 * snapshot for the lifetime of one validation call. Reload swaps the whole
 * snapshot in one operation; no concurrent mutation of the map.
 */
@Component
public class JwtKeyStore {

    private static final Logger log = LoggerFactory.getLogger(JwtKeyStore.class);

    /**
     * Immutable snapshot of the key set. {@link JwtTokenProvider} reads this
     * field on every sign / verify; rotation swaps a NEW snapshot into the
     * {@link AtomicReference}.
     */
    public static final class Snapshot {
        private final String currentKid;
        private final Map<String, SecretKey> keys;
        /** Used to detect "nothing changed" on poll without parsing JSON. */
        private final int contentHash;

        Snapshot(String currentKid, Map<String, SecretKey> keys, int contentHash) {
            this.currentKid = Objects.requireNonNull(currentKid, "currentKid");
            this.keys = Collections.unmodifiableMap(new HashMap<>(keys));
            this.contentHash = contentHash;
            if (!this.keys.containsKey(currentKid)) {
                throw new IllegalArgumentException(
                        "currentKid=" + currentKid + " not in keys map " + this.keys.keySet());
            }
        }

        public String currentKid() {
            return currentKid;
        }

        public SecretKey keyFor(String kid) {
            return keys.get(kid);
        }

        public SecretKey currentKey() {
            return keys.get(currentKid);
        }

        public java.util.Set<String> kids() {
            return keys.keySet();
        }
    }

    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>();
    private final String sidecarPath;
    private final long pollIntervalMs;
    private ScheduledExecutorService poller;

    // @Autowired — same reason as JwtTokenProvider's main ctor: two ctors exist
    // (this + the test-only seed-from-map one); without the annotation Spring
    // can't pick.
    @org.springframework.beans.factory.annotation.Autowired
    public JwtKeyStore(
            @Value("${app.jwt.kid-current:v1}") String currentKid,
            @Value("${app.jwt.kid-previous:}") String previousKid,
            @Value("${app.jwt.keys.v1:${app.jwt.secret:dev-only-secret-please-rotate-in-prod-32+chars-needed-for-HS256}}")
            String keyV1,
            @Value("${app.jwt.keys.v2:}") String keyV2,
            @Value("${app.jwt.key-store.path:}") String sidecarPath,
            @Value("${app.jwt.key-store.poll-interval-ms:60000}") long pollIntervalMs) {
        Map<String, SecretKey> seed = new HashMap<>();
        registerInto(seed, "v1", keyV1);
        registerInto(seed, "v2", keyV2);
        if (!seed.containsKey(currentKid)) {
            throw new IllegalArgumentException(
                    "app.jwt.kid-current=" + currentKid + " has no key configured under app.jwt.keys");
        }
        if (previousKid != null && !previousKid.isBlank() && !seed.containsKey(previousKid)) {
            throw new IllegalArgumentException(
                    "app.jwt.kid-previous=" + previousKid + " has no key configured under app.jwt.keys");
        }
        this.snapshot.set(new Snapshot(currentKid, seed, 0));
        this.sidecarPath = sidecarPath;
        this.pollIntervalMs = pollIntervalMs;
    }

    /**
     * Test-only seam: construct a store from an explicit map. Lets unit tests
     * exercise dual-key validation without dragging Spring into the picture.
     */
    public JwtKeyStore(String currentKid, Map<String, String> rawKeysByKid) {
        Map<String, SecretKey> seed = new HashMap<>();
        for (Map.Entry<String, String> e : rawKeysByKid.entrySet()) {
            registerInto(seed, e.getKey(), e.getValue());
        }
        this.snapshot.set(new Snapshot(currentKid, seed, 0));
        this.sidecarPath = "";
        this.pollIntervalMs = 0L;
    }

    @PostConstruct
    void startPoller() {
        if (sidecarPath == null || sidecarPath.isBlank()) {
            log.info("[jwt-key-store] no app.jwt.key-store.path configured — using inline keys, no hot-reload");
            return;
        }
        // Attempt one synchronous load so the very first verification picks up
        // the on-disk state. A failure here is non-fatal: the inline seed map
        // is still serviceable.
        try {
            reloadFromSidecar();
            log.info("[jwt-key-store] loaded keys from sidecar {} — current kid={}, kids={}",
                    sidecarPath, snapshot.get().currentKid(), snapshot.get().kids());
        } catch (Exception e) {
            log.warn("[jwt-key-store] initial sidecar load from {} failed; continuing with inline seed. error={}",
                    sidecarPath, e.toString());
        }
        if (pollIntervalMs <= 0) {
            log.info("[jwt-key-store] poll interval <= 0; will not hot-reload (boot-time only)");
            return;
        }
        poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jwt-key-store-poller");
            t.setDaemon(true);
            return t;
        });
        poller.scheduleWithFixedDelay(this::reloadFromSidecarQuiet,
                pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
        log.info("[jwt-key-store] polling {} every {} ms", sidecarPath, pollIntervalMs);
    }

    @PreDestroy
    void stopPoller() {
        if (poller != null) {
            poller.shutdownNow();
        }
    }

    public Snapshot current() {
        return snapshot.get();
    }

    /** Visible for tests — force a reload from the configured sidecar. */
    void reloadFromSidecar() throws IOException {
        Path p = Path.of(sidecarPath);
        if (!Files.isReadable(p)) {
            throw new IOException("sidecar not readable: " + sidecarPath);
        }
        byte[] bytes = Files.readAllBytes(p);
        int hash = java.util.Arrays.hashCode(bytes);
        Snapshot prev = snapshot.get();
        if (prev != null && prev.contentHash == hash) {
            return; // no-op
        }
        Snapshot fresh = parse(bytes, hash);
        snapshot.set(fresh);
        if (prev == null || !prev.currentKid().equals(fresh.currentKid()) || !prev.kids().equals(fresh.kids())) {
            log.info("[jwt-key-store] reloaded sidecar — current kid={}, kids={}",
                    fresh.currentKid(), fresh.kids());
        }
    }

    private void reloadFromSidecarQuiet() {
        try {
            reloadFromSidecar();
        } catch (Exception e) {
            // A polling failure must NEVER take the gateway down. Worst case the
            // in-memory snapshot stays stale; ops sees the warn log and the
            // Secret Manager fetch alert.
            log.warn("[jwt-key-store] poll reload failed; keeping previous snapshot. error={}", e.toString());
        }
    }

    /**
     * Minimal JSON parser for the sidecar — avoids dragging Jackson into the
     * security package. Schema is fixed: a top-level object with string
     * {@code current}, optional {@code previous} array of strings, and a
     * {@code keys} object whose values are raw key strings (≥32 bytes UTF-8).
     */
    static Snapshot parse(byte[] bytes, int contentHash) {
        // We intentionally use Spring's already-on-the-classpath Jackson via a
        // tiny reflection-free helper. Spring Boot brings Jackson transitively;
        // it's effectively free.
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            String currentKid = textOrThrow(root, "current");
            com.fasterxml.jackson.databind.JsonNode keysNode = root.get("keys");
            if (keysNode == null || !keysNode.isObject()) {
                throw new IllegalArgumentException("sidecar missing 'keys' object");
            }
            Map<String, SecretKey> map = new HashMap<>();
            java.util.Iterator<Map.Entry<String, com.fasterxml.jackson.databind.JsonNode>> it = keysNode.fields();
            while (it.hasNext()) {
                Map.Entry<String, com.fasterxml.jackson.databind.JsonNode> e = it.next();
                registerInto(map, e.getKey(), e.getValue().asText());
            }
            return new Snapshot(currentKid, map, contentHash);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("sidecar JSON parse failed: " + ioe.getMessage(), ioe);
        }
    }

    private static String textOrThrow(com.fasterxml.jackson.databind.JsonNode root, String field) {
        com.fasterxml.jackson.databind.JsonNode n = root.get(field);
        if (n == null || !n.isTextual() || n.asText().isBlank()) {
            throw new IllegalArgumentException("sidecar missing or blank '" + field + "'");
        }
        return n.asText();
    }

    private static void registerInto(Map<String, SecretKey> target, String kid, String secret) {
        if (secret == null || secret.isBlank()) {
            return;
        }
        byte[] raw = secret.getBytes(StandardCharsets.UTF_8);
        if (raw.length < 32) {
            throw new IllegalArgumentException(
                    "app.jwt.keys." + kid + " must be at least 32 bytes for HS256 (got " + raw.length + ")");
        }
        target.put(kid, Keys.hmacShaKeyFor(raw));
    }
}
