package com.onlinejudge.sandbox.service;

import com.onlinejudge.sandbox.model.Sandbox;
import com.onlinejudge.sandbox.model.SandboxState;
import com.onlinejudge.sandbox.observability.SandboxMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the wiring of {@link NetnsApplier} into {@link LeaseService}'s
 * lease/release lifecycle (roadmap §3.1). The Firecracker boot path is
 * never invoked here — the pool is seeded with a pre-built {@link
 * Sandbox} so lease() flows straight through to the netns + cgroup
 * + watchdog wiring under test.
 */
class LeaseServiceEgressLockdownTest {

    private PoolManager pool;
    private WatchdogService watchdog;
    private CgroupApplier cgroups;
    private SandboxMetrics metrics;
    private NetnsApplier netns;

    @BeforeEach
    void setUp() {
        pool = mock(PoolManager.class);
        watchdog = mock(WatchdogService.class);
        cgroups = mock(CgroupApplier.class);
        metrics = mock(SandboxMetrics.class);
        netns = mock(NetnsApplier.class);
    }

    /** Build a LeaseService with the canonical defaults the prod
     *  application.yml uses (egress lockdown on, vcpus=1, mem=256, …),
     *  short-circuiting Spring's @Value injection via reflection. */
    private LeaseService newService(boolean lockdownEnabled) {
        LeaseService svc = new LeaseService(pool, watchdog, cgroups, metrics, netns);
        ReflectionTestUtils.setField(svc, "egressLockdownEnabled", lockdownEnabled);
        ReflectionTestUtils.setField(svc, "defaultMemoryMib", 256);
        ReflectionTestUtils.setField(svc, "defaultLeaseWallSeconds", 30);
        ReflectionTestUtils.setField(svc, "defaultCpuPeriodUs", 100_000L);
        ReflectionTestUtils.setField(svc, "defaultCpuQuotaUs", 100_000L);
        return svc;
    }

    private Sandbox seededSandbox(String id, String language) {
        Sandbox sb = new Sandbox(id, language, 4321L,
                "/tmp/" + id + ".sock", "/tmp/" + id + "-vsock.sock", "placeholder-tok");
        sb.transition(SandboxState.READY);
        return sb;
    }

    // ------------------------------------------------------------------

    @Test
    void lease_whenLockdownEnabled_createsAndConfiguresNetns() {
        LeaseService svc = newService(true);
        Sandbox sb = seededSandbox("sb-leased", "python");
        when(pool.acquire("python")).thenReturn(sb);

        Sandbox out = svc.lease("sub-1", "python", null);

        assertThat(out.sandboxId()).isEqualTo("sb-leased");
        verify(netns, times(1)).create("sb-leased");
        verify(netns, times(1)).installIptablesRules("sb-leased");
        verify(netns, never()).destroy(anyString());
    }

    @Test
    void release_whenLockdownEnabled_destroysNetnsExactlyOnce() {
        LeaseService svc = newService(true);
        Sandbox sb = seededSandbox("sb-rel", "java");
        when(pool.acquire("java")).thenReturn(sb);

        svc.lease("sub-2", "java", null);
        svc.release("sb-rel");

        verify(netns).create("sb-rel");
        verify(netns, times(1)).destroy("sb-rel");
        verify(netns, times(1)).removeIptablesRules("sb-rel");
    }

    @Test
    void lease_whenLockdownDisabled_neverTouchesNetns() {
        LeaseService svc = newService(false);
        Sandbox sb = seededSandbox("sb-legacy", "cpp");
        when(pool.acquire("cpp")).thenReturn(sb);

        svc.lease("sub-3", "cpp", null);
        svc.release("sb-legacy");

        verify(netns, never()).create(anyString());
        verify(netns, never()).destroy(anyString());
        verify(netns, never()).installIptablesRules(anyString());
        verify(netns, never()).removeIptablesRules(anyString());
    }

    @Test
    void lease_midFlightFailure_stillDestroysNetns() {
        LeaseService svc = newService(true);
        Sandbox sb = seededSandbox("sb-bad", "python");
        when(pool.acquire("python")).thenReturn(sb);
        // Simulate a failure inside the lease pipeline by making the
        // cgroup applier throw — this happens AFTER netnsApplier.create
        // but BEFORE the watchdog schedule, exercising the catch path.
        doThrow(new RuntimeException("cgroup boom"))
                .when(cgroups).applyAtLease(eq(sb), anyLong(), anyLong(), anyLong());

        assertThatThrownBy(() -> svc.lease("sub-bad", "python", null))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("cgroup boom");

        // create was called once; destroy was called at least once
        // (catch path always destroys; if release also ran via forceKill
        // the count can be 2, both acceptable — the namespace just has
        // to be gone).
        ArgumentCaptor<String> cap = ArgumentCaptor.forClass(String.class);
        verify(netns).create("sb-bad");
        verify(netns, times(1)).installIptablesRules("sb-bad");
        // The comment above explicitly says "1 or 2 calls acceptable" — match
        // that intent with atLeastOnce(). The catch path always destroys; if
        // the mid-flight forceKill→release also fires (the prod safety net),
        // we get 2 destroys, which is what we want.
        verify(netns, atLeastOnce()).destroy(cap.capture());
        assertThat(cap.getValue()).isEqualTo("sb-bad");
    }

    @Test
    void poolExhausted_neverTouchesNetns() {
        LeaseService svc = newService(true);
        when(pool.acquire("python")).thenReturn(null);
        when(pool.retryAfterMsForLanguage("python")).thenReturn(250L);

        assertThatThrownBy(() -> svc.lease("sub-x", "python", null))
                .isInstanceOf(PoolExhaustedException.class);

        verify(netns, never()).create(anyString());
        verify(netns, never()).destroy(anyString());
    }
}
