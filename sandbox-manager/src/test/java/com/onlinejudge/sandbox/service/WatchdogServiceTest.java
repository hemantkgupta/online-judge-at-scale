package com.onlinejudge.sandbox.service;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WatchdogService}. Uses a {@link CountDownLatch}
 * for the fire-after-delay assertion and an {@link AtomicBoolean} for
 * the cancel-prevents-fire one. The pid-kill is the LeaseService's
 * concern (the watchdog only schedules a {@code Runnable}); we verify
 * the callback was invoked, not how.
 */
class WatchdogServiceTest {

    @Test
    void schedule_firesAfterDelay() throws Exception {
        WatchdogService w = new WatchdogService();
        try {
            CountDownLatch fired = new CountDownLatch(1);
            w.schedule("sb-1", System.currentTimeMillis() + 80, fired::countDown);
            assertThat(fired.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            w.shutdown();
        }
    }

    @Test
    void cancel_preventsFire() throws Exception {
        WatchdogService w = new WatchdogService();
        try {
            AtomicBoolean fired = new AtomicBoolean(false);
            w.schedule("sb-2", System.currentTimeMillis() + 500, () -> fired.set(true));
            boolean cancelled = w.cancel("sb-2");
            assertThat(cancelled).isTrue();
            // Wait past the scheduled fire time to be sure.
            Thread.sleep(700);
            assertThat(fired.get()).isFalse();
            assertThat(w.activeCount()).isEqualTo(0);
        } finally {
            w.shutdown();
        }
    }

    @Test
    void cancel_unknownSandbox_isNoOp() {
        WatchdogService w = new WatchdogService();
        try {
            assertThat(w.cancel("sb-unknown")).isFalse();
        } finally {
            w.shutdown();
        }
    }

    @Test
    void rescheduleSameSandbox_cancelsPrevious() throws Exception {
        WatchdogService w = new WatchdogService();
        try {
            AtomicBoolean first = new AtomicBoolean(false);
            CountDownLatch second = new CountDownLatch(1);
            w.schedule("sb-3", System.currentTimeMillis() + 1_000, () -> first.set(true));
            w.schedule("sb-3", System.currentTimeMillis() + 50, second::countDown);
            assertThat(second.await(2, TimeUnit.SECONDS)).isTrue();
            // Wait past the first scheduled time; it should never fire.
            Thread.sleep(1_200);
            assertThat(first.get()).isFalse();
        } finally {
            w.shutdown();
        }
    }
}
