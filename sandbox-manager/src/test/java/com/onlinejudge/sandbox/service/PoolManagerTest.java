package com.onlinejudge.sandbox.service;

import com.onlinejudge.sandbox.model.Sandbox;
import com.onlinejudge.sandbox.model.SandboxState;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link PoolManager}. We disable the replenish loop
 * (Spring's scheduler isn't running here anyway, but we set enabled=false
 * to be explicit) and exercise {@link PoolManager#acquire} directly by
 * seeding the deque via {@link PoolManager#offer}.
 */
class PoolManagerTest {

    private PoolManager fresh() {
        @SuppressWarnings("unchecked")
        ObjectProvider<LeaseService> provider = mock(ObjectProvider.class);
        PoolManager pm = new PoolManager(provider);
        pm.setEnabled(false);
        return pm;
    }

    @Test
    void acquire_returnsNullWhenPoolEmpty() {
        PoolManager pm = fresh();
        assertThat(pm.acquire("python")).isNull();
        assertThat(pm.readyCount("python")).isEqualTo(0);
    }

    @Test
    void acquire_popsAndDecrementsDepth() {
        PoolManager pm = fresh();
        Sandbox a = warmSandbox("sb-a", "python");
        Sandbox b = warmSandbox("sb-b", "python");
        pm.offer("python", a);
        pm.offer("python", b);
        assertThat(pm.readyCount("python")).isEqualTo(2);

        Sandbox first = pm.acquire("python");
        assertThat(first).isNotNull();
        assertThat(pm.readyCount("python")).isEqualTo(1);

        Sandbox second = pm.acquire("python");
        assertThat(second).isNotNull();
        assertThat(first.sandboxId()).isNotEqualTo(second.sandboxId());
        assertThat(pm.readyCount("python")).isEqualTo(0);

        assertThat(pm.acquire("python")).isNull();
    }

    @Test
    void acquire_isPerLanguage() {
        PoolManager pm = fresh();
        pm.offer("python", warmSandbox("sb-p", "python"));
        pm.offer("cpp",    warmSandbox("sb-c", "cpp"));

        assertThat(pm.acquire("java")).isNull();
        assertThat(pm.acquire("python")).isNotNull();
        assertThat(pm.acquire("python")).isNull();
        assertThat(pm.acquire("cpp")).isNotNull();
    }

    @Test
    void retryAfterMs_isExposedToCaller() {
        PoolManager pm = fresh();
        pm.setRetryAfterMs(500);
        assertThat(pm.retryAfterMsForLanguage("python")).isEqualTo(500);
    }

    private static Sandbox warmSandbox(String id, String lang) {
        Sandbox sb = new Sandbox(id, lang, 1234L,
                "/tmp/" + id + ".sock", "/tmp/" + id + "-vsock.sock", "tok");
        sb.transition(SandboxState.READY);
        return sb;
    }
}
