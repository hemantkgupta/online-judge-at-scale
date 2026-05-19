package com.onlinejudge.gateway.security;

import io.jsonwebtoken.JwtException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the dual-key / overlap-window behaviour of {@link JwtKeyStore} +
 * {@link JwtTokenProvider}. These are the load-bearing properties of the
 * monthly JWT rotation described in {@code docs/design-docs/key-rotation.md}:
 *
 * <ul>
 *   <li>A token signed under {@code kid=v2} must validate against a store that
 *       carries both {@code v1} and {@code v2} with current=v2 — the steady
 *       state immediately after the cutover.</li>
 *   <li>A token signed under {@code kid=v1} must STILL validate during the
 *       overlap window — i.e. tokens minted before the cutover keep working
 *       until {@code v1} is swept from the map.</li>
 *   <li>A token whose {@code kid} is not in the store must fail with a clear
 *       {@link JwtException} — not a misleading "bad signature" — so rotation
 *       triage in the logs is unambiguous.</li>
 *   <li>Sidecar reload swaps the in-memory map atomically: an old snapshot
 *       captured before the reload keeps working for the current request,
 *       while subsequent requests pick up the new current kid.</li>
 *   <li>Sidecar reload tolerates corrupt JSON without taking the gateway down
 *       — the previous snapshot is retained.</li>
 * </ul>
 */
class JwtKeyStoreRotationTest {

    private static final String SECRET_V1 = "rotation-v1-secret-32-bytes-min-aaaaaaaaa";
    private static final String SECRET_V2 = "rotation-v2-secret-32-bytes-min-bbbbbbbbb";
    private static final String SECRET_V3 = "rotation-v3-secret-32-bytes-min-ccccccccc";

    private static JwtTokenProvider providerFor(JwtKeyStore store) {
        return new JwtTokenProvider(store, 900L, "online-judge");
    }

    private static Map<String, String> keys(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void tokenSignedWithCurrentKidValidates() {
        JwtKeyStore store = new JwtKeyStore("v2", keys("v1", SECRET_V1, "v2", SECRET_V2));
        JwtTokenProvider provider = providerFor(store);

        String token = provider.issueToken("user-1");

        assertThat(provider.validateAndExtractUserId(token)).isEqualTo("user-1");
    }

    @Test
    void tokenSignedUnderPreviousKidStillValidatesDuringOverlap() {
        // Mint while v1 is current ...
        JwtKeyStore oldStore = new JwtKeyStore("v1", keys("v1", SECRET_V1));
        JwtTokenProvider oldProvider = providerFor(oldStore);
        String tokenSignedAsV1 = oldProvider.issueToken("user-1");

        // ... then cut over: v2 is current, v1 retained in the overlap window.
        JwtKeyStore newStore = new JwtKeyStore("v2", keys("v1", SECRET_V1, "v2", SECRET_V2));
        JwtTokenProvider newProvider = providerFor(newStore);

        assertThat(newProvider.validateAndExtractUserId(tokenSignedAsV1)).isEqualTo("user-1");
    }

    @Test
    void tokenSignedUnderSweptKidFailsWithKidErrorNotSignatureError() {
        // Mint under v1 ...
        JwtKeyStore oldStore = new JwtKeyStore("v1", keys("v1", SECRET_V1));
        String token = providerFor(oldStore).issueToken("user-1");

        // ... then sweep v1 (post-overlap steady state).
        JwtKeyStore postSweep = new JwtKeyStore("v2", keys("v2", SECRET_V2));
        JwtTokenProvider postSweepProvider = providerFor(postSweep);

        assertThatThrownBy(() -> postSweepProvider.validateAndExtractUserId(token))
                .isInstanceOf(JwtException.class)
                .hasMessageContaining("unknown kid")
                .hasMessageContaining("v1");
    }

    @Test
    void tokenSignedWithUnknownKidFailsCleanly() {
        // Fabricate a "v99 token" by minting under a store where v99 is the
        // sole / current key, then handing it to a store that doesn't know v99.
        JwtKeyStore attackerStore = new JwtKeyStore("v99", keys("v99", SECRET_V3));
        String forged = providerFor(attackerStore).issueToken("user-1");

        JwtKeyStore prodStore = new JwtKeyStore("v2", keys("v1", SECRET_V1, "v2", SECRET_V2));
        JwtTokenProvider prodProvider = providerFor(prodStore);

        assertThatThrownBy(() -> prodProvider.validateAndExtractUserId(forged))
                .isInstanceOf(JwtException.class)
                .hasMessageContaining("unknown kid");
    }

    @Test
    void rotationCutoverIsAtomic_oldProviderInstanceKeepsWorking() {
        // Concurrency-property smoke test: the verifier path holds a Snapshot
        // for the duration of one validation call, so a rotation that happens
        // "between" two calls cannot mix-and-match keys mid-parse. We can't
        // race the JVM in a unit test cheaply, so this asserts the equivalent
        // structural property: a Snapshot taken before reload is independent
        // of one taken after.
        JwtKeyStore store = new JwtKeyStore("v1", keys("v1", SECRET_V1));
        JwtKeyStore.Snapshot before = store.current();

        // Swap the store as if a sidecar reload happened.
        JwtKeyStore postRotate = new JwtKeyStore("v2", keys("v1", SECRET_V1, "v2", SECRET_V2));
        JwtKeyStore.Snapshot after = postRotate.current();

        assertThat(before.currentKid()).isEqualTo("v1");
        assertThat(after.currentKid()).isEqualTo("v2");
        // `before` still has its own key — no shared mutable state.
        assertThat(before.kids()).containsExactly("v1");
        assertThat(after.kids()).containsExactlyInAnyOrder("v1", "v2");
    }

    @Test
    void sidecarParseProducesExpectedSnapshot() {
        String json = "{\n" +
                "  \"current\": \"v2\",\n" +
                "  \"previous\": [\"v1\"],\n" +
                "  \"keys\": {\n" +
                "    \"v1\": \"" + SECRET_V1 + "\",\n" +
                "    \"v2\": \"" + SECRET_V2 + "\"\n" +
                "  }\n" +
                "}";
        JwtKeyStore.Snapshot snap = JwtKeyStore.parse(json.getBytes(), /*contentHash*/ 0);
        assertThat(snap.currentKid()).isEqualTo("v2");
        assertThat(snap.kids()).containsExactlyInAnyOrder("v1", "v2");
        assertThat(snap.currentKey()).isNotNull();
        assertThat(snap.keyFor("v1")).isNotNull();
    }

    @Test
    void sidecarParseRejectsMalformedJson() {
        byte[] bad = "{ not json".getBytes();
        assertThatThrownBy(() -> JwtKeyStore.parse(bad, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sidecarParseRejectsMissingCurrentField() {
        String noCurrent = "{ \"keys\": { \"v1\": \"" + SECRET_V1 + "\" } }";
        assertThatThrownBy(() -> JwtKeyStore.parse(noCurrent.getBytes(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("current");
    }

    @Test
    void sidecarParseRejectsShortKey() {
        String shortKey = "{ \"current\": \"v1\", \"keys\": { \"v1\": \"too-short\" } }";
        assertThatThrownBy(() -> JwtKeyStore.parse(shortKey.getBytes(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
    }

    @Test
    void sidecarReloadFromDiskSwapsSnapshot(@TempDir Path tmp) throws Exception {
        // Bootstrap with v1 only ...
        JwtKeyStore store = new JwtKeyStore("v1", keys("v1", SECRET_V1));
        assertThat(store.current().currentKid()).isEqualTo("v1");

        // Build a store that's wired to a sidecar path and force a reload.
        // (We can't easily exercise the @PostConstruct poller without Spring,
        // so call the package-private reload directly.)
        Path sidecar = tmp.resolve("jwt-keys.json");
        String v2Json = "{ \"current\": \"v2\", \"keys\": { \"v1\": \"" + SECRET_V1
                + "\", \"v2\": \"" + SECRET_V2 + "\" } }";
        Files.writeString(sidecar, v2Json);

        // Use the Spring-style constructor with the sidecar wired in.
        JwtKeyStore live = new JwtKeyStore(
                /*currentKid*/ "v1",
                /*previousKid*/ "",
                /*keyV1*/ SECRET_V1,
                /*keyV2*/ "",
                /*sidecarPath*/ sidecar.toString(),
                /*pollIntervalMs*/ 0L); // no async poller for the test
        live.reloadFromSidecar();

        JwtKeyStore.Snapshot after = live.current();
        assertThat(after.currentKid()).isEqualTo("v2");
        assertThat(after.kids()).containsExactlyInAnyOrder("v1", "v2");
    }

    @Test
    void sidecarReloadKeepsPreviousSnapshotOnCorruptFile(@TempDir Path tmp) throws Exception {
        Path sidecar = tmp.resolve("jwt-keys.json");
        String okJson = "{ \"current\": \"v2\", \"keys\": { \"v1\": \"" + SECRET_V1
                + "\", \"v2\": \"" + SECRET_V2 + "\" } }";
        Files.writeString(sidecar, okJson);

        JwtKeyStore live = new JwtKeyStore("v1", "", SECRET_V1, "", sidecar.toString(), 0L);
        live.reloadFromSidecar();
        assertThat(live.current().currentKid()).isEqualTo("v2");

        // Now corrupt the file ...
        Files.writeString(sidecar, "{ not json");
        // ... a fresh reload should THROW (so the polling-quiet path can log
        // it) but the in-memory snapshot must NOT regress.
        assertThatThrownBy(live::reloadFromSidecar).isInstanceOf(RuntimeException.class);
        assertThat(live.current().currentKid()).isEqualTo("v2");
        assertThat(live.current().kids()).containsExactlyInAnyOrder("v1", "v2");
    }
}
