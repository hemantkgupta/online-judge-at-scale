package com.onlinejudge.worker.integration;

import com.onlinejudge.worker.sandbox.Sandbox;
import com.onlinejudge.worker.sandbox.SandboxPool;
import com.onlinejudge.worker.sandbox.SandboxState;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test for the sandbox warm pool under high concurrency.
 *
 * Simulates the final-minute surge: many execution workers competing
 * for sandboxes simultaneously. Verifies:
 *
 * 1. No sandbox is leased to two different submissions (thread safety)
 * 2. Pool metrics remain consistent under concurrent operations
 * 3. The pool handles exhaustion gracefully (returns null, not error)
 * 4. Rapid lease/release cycles don't corrupt pool state
 * 5. Concurrent provisioning + leasing don't deadlock
 *
 * This is the most critical concurrency test in the execution tier.
 * The warm pool is the bottleneck during the surge — any race condition
 * here means either duplicate execution (waste) or lost submissions (bug).
 */
class SandboxPoolStressTest {

    @Test
    void concurrentLease_100threads_50sandboxes_noDuplicates() throws Exception {
        SandboxPool pool = new SandboxPool("python", 50);

        // Pre-provision 50 sandboxes
        for (int i = 0; i < 50; i++) {
            Sandbox s = new Sandbox("python");
            s.markReady("container-" + i);
            pool.addReady(s);
        }

        // 100 threads compete for 50 sandboxes
        int threadCount = 100;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        Set<String> leasedSandboxIds = ConcurrentHashMap.newKeySet();
        Set<String> leasedSubmissionIds = ConcurrentHashMap.newKeySet();
        AtomicInteger nullCount = new AtomicInteger(0);

        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final String submissionId = "sub-" + i;
            exec.submit(() -> {
                try {
                    startGate.await(); // All threads start at once (simulate surge)
                    Sandbox s = pool.tryLease(submissionId);
                    if (s != null) {
                        leasedSandboxIds.add(s.getId());
                        leasedSubmissionIds.add(submissionId);
                        assertThat(s.getState()).isEqualTo(SandboxState.LEASED);
                        assertThat(s.getSubmissionId()).isEqualTo(submissionId);
                    } else {
                        nullCount.incrementAndGet();
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    done.countDown();
                }
            });
        }

        startGate.countDown(); // Release all threads simultaneously
        done.await(10, TimeUnit.SECONDS);
        exec.shutdown();

        // Exactly 50 sandboxes leased, 50 threads got null
        assertThat(leasedSandboxIds).hasSize(50);
        assertThat(leasedSubmissionIds).hasSize(50);
        assertThat(nullCount.get()).isEqualTo(50);
        assertThat(pool.readyCount()).isEqualTo(0);
        assertThat(pool.leasedCount()).isEqualTo(50);
    }

    @Test
    void rapidLeaseRelease_metricsRemainConsistent() throws Exception {
        SandboxPool pool = new SandboxPool("java", 20);

        // Run 10 rounds of provision → lease → terminate
        for (int round = 0; round < 10; round++) {
            // Provision 20 sandboxes
            List<Sandbox> sandboxes = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                Sandbox s = new Sandbox("java");
                s.markReady("container-r" + round + "-" + i);
                pool.addReady(s);
                sandboxes.add(s);
            }

            // Lease all 20
            List<Sandbox> leased = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                Sandbox s = pool.tryLease("sub-r" + round + "-" + i);
                assertThat(s).isNotNull();
                leased.add(s);
            }

            // Pool should be exhausted
            assertThat(pool.tryLease("sub-extra")).isNull();
            assertThat(pool.readyCount()).isEqualTo(0);
            assertThat(pool.leasedCount()).isEqualTo(20);

            // Release all
            for (Sandbox s : leased) {
                s.markDirty();
                s.markTerminated();
                pool.recordTerminated();
            }

            // Pool should be empty but metrics consistent
            assertThat(pool.readyCount()).isEqualTo(0);
            assertThat(pool.leasedCount()).isEqualTo(0);
        }

        // After 10 rounds: 200 total created, 200 total terminated
        assertThat(pool.totalCreated()).isEqualTo(200);
        assertThat(pool.totalTerminated()).isEqualTo(200);
    }

    @Test
    void concurrentProvisioningAndLeasing_noDeadlock() throws Exception {
        SandboxPool pool = new SandboxPool("cpp", 30);

        int provisionerCount = 5;
        int consumerCount = 20;
        CountDownLatch done = new CountDownLatch(provisionerCount + consumerCount);
        AtomicInteger successfulLeases = new AtomicInteger(0);

        ExecutorService exec = Executors.newFixedThreadPool(provisionerCount + consumerCount);

        // 5 provisioner threads continuously adding sandboxes
        for (int p = 0; p < provisionerCount; p++) {
            final int provId = p;
            exec.submit(() -> {
                try {
                    for (int i = 0; i < 20; i++) {
                        Sandbox s = new Sandbox("cpp");
                        s.markReady("container-p" + provId + "-" + i);
                        pool.addReady(s);
                        Thread.sleep(1); // Simulate 1ms provisioning time
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }

        // 20 consumer threads continuously trying to lease
        for (int c = 0; c < consumerCount; c++) {
            final int conId = c;
            exec.submit(() -> {
                try {
                    for (int i = 0; i < 10; i++) {
                        Sandbox s = pool.tryLease("sub-c" + conId + "-" + i);
                        if (s != null) {
                            successfulLeases.incrementAndGet();
                            // Simulate execution time
                            Thread.sleep(2);
                            s.markDirty();
                            s.markTerminated();
                            pool.recordTerminated();
                        }
                        Thread.sleep(1);
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }

        boolean completed = done.await(30, TimeUnit.SECONDS);
        exec.shutdown();

        assertThat(completed).isTrue(); // No deadlock
        assertThat(successfulLeases.get()).isGreaterThan(0);

        // Metrics should be consistent: created >= leased + ready
        int totalCreated = pool.totalCreated();
        int currentReady = pool.readyCount();
        int currentLeased = pool.leasedCount();
        int totalTerminated = pool.totalTerminated();

        assertThat(totalCreated).isGreaterThanOrEqualTo(totalTerminated);
        assertThat(currentLeased).isGreaterThanOrEqualTo(0);
    }

    @Test
    void sandbox_stateTransitions_threadSafe() throws Exception {
        // Multiple threads try to lease the same sandbox
        Sandbox sandbox = new Sandbox("python");
        sandbox.markReady("container-1");

        int threadCount = 50;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final String subId = "sub-" + i;
            exec.submit(() -> {
                try {
                    startGate.await();
                    if (sandbox.tryLease(subId)) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }

        startGate.countDown();
        done.await(5, TimeUnit.SECONDS);
        exec.shutdown();

        // Exactly one thread should win the lease
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(sandbox.getState()).isEqualTo(SandboxState.LEASED);
    }

    @Test
    void pool_deficitComputation_underConcurrency() throws Exception {
        SandboxPool pool = new SandboxPool("python", 10);

        // Add 10 sandboxes — pair each addReady() with the corresponding
        // recordProvisioning() the real boot loop would have made; addReady()
        // decrements the provisioning counter.
        for (int i = 0; i < 10; i++) {
            pool.recordProvisioning();
            Sandbox s = new Sandbox("python");
            s.markReady("c-" + i);
            pool.addReady(s);
        }
        assertThat(pool.computeDeficit()).isEqualTo(0);

        // Lease 7 concurrently
        CountDownLatch done = new CountDownLatch(7);
        ExecutorService exec = Executors.newFixedThreadPool(7);
        for (int i = 0; i < 7; i++) {
            final int idx = i;
            exec.submit(() -> {
                pool.tryLease("sub-" + idx);
                done.countDown();
            });
        }
        done.await(5, TimeUnit.SECONDS);
        exec.shutdown();

        // Deficit should be 7 (target 10, ready 3, provisioning 0)
        assertThat(pool.computeDeficit()).isEqualTo(7);
        assertThat(pool.readyCount()).isEqualTo(3);
        assertThat(pool.leasedCount()).isEqualTo(7);
    }
}
