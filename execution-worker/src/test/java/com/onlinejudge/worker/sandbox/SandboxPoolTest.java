package com.onlinejudge.worker.sandbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the thread-safe sandbox warm pool.
 *
 * Verifies:
 * - Lease returns a READY sandbox
 * - Lease returns null when pool is exhausted
 * - Multiple leases return distinct sandboxes
 * - Concurrent leases don't produce duplicates (thread safety)
 * - Pool metrics are accurate
 * - Deficit computation is correct
 * - Pool utilization tracking works
 */
class SandboxPoolTest {

    private SandboxPool pool;

    @BeforeEach
    void setUp() {
        pool = new SandboxPool("python", 5);
    }

    private Sandbox addReadySandbox() {
        // Real lifecycle: recordProvisioning() then addReady() once the sandbox is booted.
        // addReady() decrements the provisioning counter, so callers must record provisioning first
        // or the counter goes negative and computeDeficit() reads incorrectly.
        pool.recordProvisioning();
        Sandbox s = new Sandbox("python");
        s.markReady("container-" + s.getId());
        pool.addReady(s);
        return s;
    }

    @Test
    void tryLease_returnsReadySandbox() {
        addReadySandbox();

        Sandbox leased = pool.tryLease("sub-1");

        assertThat(leased).isNotNull();
        assertThat(leased.getState()).isEqualTo(SandboxState.LEASED);
        assertThat(leased.getSubmissionId()).isEqualTo("sub-1");
        assertThat(leased.getSessionToken()).isNotBlank();
    }

    @Test
    void tryLease_returnsNull_whenPoolExhausted() {
        // Empty pool
        Sandbox result = pool.tryLease("sub-1");

        assertThat(result).isNull();
    }

    @Test
    void tryLease_multipleLeases_returnDistinctSandboxes() {
        addReadySandbox();
        addReadySandbox();
        addReadySandbox();

        Sandbox s1 = pool.tryLease("sub-1");
        Sandbox s2 = pool.tryLease("sub-2");
        Sandbox s3 = pool.tryLease("sub-3");

        assertThat(s1).isNotNull();
        assertThat(s2).isNotNull();
        assertThat(s3).isNotNull();
        assertThat(s1.getId()).isNotEqualTo(s2.getId());
        assertThat(s2.getId()).isNotEqualTo(s3.getId());
    }

    @Test
    void tryLease_exhaustsPool_thenReturnsNull() {
        addReadySandbox();

        Sandbox first = pool.tryLease("sub-1");
        Sandbox second = pool.tryLease("sub-2");

        assertThat(first).isNotNull();
        assertThat(second).isNull(); // Pool exhausted after 1 lease
    }

    @Test
    void concurrentLeases_noDuplicates() throws InterruptedException {
        // Add 10 sandboxes
        for (int i = 0; i < 10; i++) {
            addReadySandbox();
        }

        // 20 threads compete for 10 sandboxes
        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        Set<String> leasedIds = ConcurrentHashMap.newKeySet();
        List<Sandbox> leased = new ArrayList<>();

        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            exec.submit(() -> {
                try {
                    Sandbox s = pool.tryLease("sub-" + idx);
                    if (s != null) {
                        boolean added = leasedIds.add(s.getId());
                        assertThat(added).isTrue(); // No duplicate sandbox IDs
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        exec.shutdown();

        // Exactly 10 sandboxes should have been leased
        assertThat(leasedIds).hasSize(10);
    }

    @Test
    void metrics_readyCount() {
        assertThat(pool.readyCount()).isEqualTo(0);

        addReadySandbox();
        addReadySandbox();

        assertThat(pool.readyCount()).isEqualTo(2);

        pool.tryLease("sub-1");

        assertThat(pool.readyCount()).isEqualTo(1);
    }

    @Test
    void metrics_leasedCount() {
        addReadySandbox();
        addReadySandbox();

        assertThat(pool.leasedCount()).isEqualTo(0);

        pool.tryLease("sub-1");
        assertThat(pool.leasedCount()).isEqualTo(1);

        pool.tryLease("sub-2");
        assertThat(pool.leasedCount()).isEqualTo(2);
    }

    @Test
    void computeDeficit_withNoSandboxes() {
        assertThat(pool.computeDeficit()).isEqualTo(5); // target=5, ready=0
    }

    @Test
    void computeDeficit_partiallyFilled() {
        addReadySandbox();
        addReadySandbox();

        assertThat(pool.computeDeficit()).isEqualTo(3); // target=5, ready=2
    }

    @Test
    void computeDeficit_fullyFilled() {
        for (int i = 0; i < 5; i++) addReadySandbox();

        assertThat(pool.computeDeficit()).isEqualTo(0);
    }

    @Test
    void computeDeficit_accountsForProvisioning() {
        addReadySandbox();
        addReadySandbox();
        pool.recordProvisioning(); // 1 in-flight

        assertThat(pool.computeDeficit()).isEqualTo(2); // target=5, ready=2, prov=1
    }

    @Test
    void recordProvisionFailed_decrementsProvisioningCounter() {
        // Symmetric to recordProvisioning(): without this, a failed Docker boot
        // would leak an in-flight slot and computeDeficit() would under-report.
        pool.recordProvisioning();
        pool.recordProvisioning();
        assertThat(pool.provisioningCount()).isEqualTo(2);

        pool.recordProvisionFailed();
        assertThat(pool.provisioningCount()).isEqualTo(1);

        // Deficit after one success + one failure: target=5, ready=0, provisioning=1
        assertThat(pool.computeDeficit()).isEqualTo(4);
    }

    @Test
    void recordProvisionFailed_pairedWithRecordProvisioning_isZeroSum() {
        int initial = pool.provisioningCount();
        for (int i = 0; i < 10; i++) {
            pool.recordProvisioning();
            pool.recordProvisionFailed();
        }
        assertThat(pool.provisioningCount()).isEqualTo(initial);
    }

    @Test
    void utilization_emptyPool() {
        assertThat(pool.utilization()).isEqualTo(0.0);
    }

    @Test
    void utilization_halfLeased() {
        addReadySandbox();
        addReadySandbox();
        pool.tryLease("sub-1"); // 1 leased, 1 ready

        assertThat(pool.utilization()).isEqualTo(0.5);
    }

    @Test
    void isExhausted_emptyPool() {
        assertThat(pool.isExhausted()).isTrue();
    }

    @Test
    void isExhausted_withReadySandbox() {
        addReadySandbox();
        assertThat(pool.isExhausted()).isFalse();
    }

    @Test
    void isCriticallyLow_belowTwentyPercent() {
        // Target is 5, so critically low = ready < 1
        assertThat(pool.isCriticallyLow()).isTrue(); // 0 ready

        addReadySandbox();
        assertThat(pool.isCriticallyLow()).isFalse(); // 1 ready >= 1 (20% of 5)
    }
}
