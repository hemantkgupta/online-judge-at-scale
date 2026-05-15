package com.onlinejudge.sandbox.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import org.springframework.stereotype.Component;

/**
 * Sandbox-manager custom metrics (Workstream H).
 *
 * <p>All metrics are emitted via the OpenTelemetry API. The runtime SDK is
 * the one bundled with the OTel Java agent loaded via {@code JAVA_TOOL_OPTIONS}
 * (see {@code sandbox-manager/Dockerfile}). When
 * {@code OTEL_EXPORTER_OTLP_ENDPOINT} is unset, the SDK still records
 * measurements but exports nowhere — the instrumentation is effectively
 * a no-op without a collector wired in. When set, measurements ship over
 * OTLP/gRPC to whatever address the operator points at (SigNoz collector,
 * Cloud Trace agent, etc).
 *
 * <h2>Wire-in points</h2>
 * <ul>
 *   <li><b>{@link #recordLeaseLatency}</b> — wired by Workstream H in
 *       {@code LeaseService.lease(...)} from method entry to return.</li>
 *   <li><b>{@link #incActiveLeases} / {@link #decActiveLeases}</b> — wired
 *       by Workstream H in {@code LeaseService.lease(...)} at LEASED
 *       transition (inc) and in {@code LeaseService.release(...)} at
 *       TERMINATED transition (dec).</li>
 *   <li><b>{@link #incWatchdogFires}</b> — to be wired by Workstream CD's
 *       {@code WatchdogService} in its {@code onFire} callback when the
 *       wall-clock kill SIGKILLs a sandbox. <i>DO NOT edit that file from
 *       this workstream — leave the metric class here for CD to consume.</i></li>
 *   <li><b>{@link #observePoolDepth}</b> — to be wired by Workstream CD's
 *       {@code PoolManager} replenisher loop (already {@code @Scheduled} at
 *       fixedDelay=500ms). Call once per language per tick with the current
 *       READY-queue size. <i>DO NOT edit that file from this workstream.</i></li>
 * </ul>
 *
 * <h2>Metric inventory</h2>
 * <pre>
 *   sandbox.lease.latency_ms     (histogram)    attrs: language
 *   sandbox.leases.active        (up-down)      attrs: language
 *   sandbox.watchdog.fires_total (counter)      attrs: language
 *   sandbox.pool.ready           (up-down)      attrs: language
 * </pre>
 *
 * <p>Histograms use the default bucket boundaries shipped with the agent
 * SDK — fine for the wide dynamic range of lease latency (sub-ms on a
 * pool-hit, multi-second on a cold boot).
 */
@Component
public class SandboxMetrics {

    public static final AttributeKey<String> LANGUAGE = AttributeKey.stringKey("language");

    /** Instrumentation scope name — picked up by SigNoz / Cloud Trace as the
     *  source of the metric. Versioned because the agent SDK records the
     *  scope version against every measurement. */
    private static final String SCOPE = "com.onlinejudge.sandbox";
    private static final String SCOPE_VERSION = "1.0.0";

    private final DoubleHistogram leaseLatencyMs;
    private final LongUpDownCounter activeLeases;
    private final LongCounter watchdogFires;
    private final LongUpDownCounter poolDepth;

    public SandboxMetrics() {
        Meter meter = GlobalOpenTelemetry.getMeter(SCOPE);

        this.leaseLatencyMs = meter
                .histogramBuilder("sandbox.lease.latency_ms")
                .setDescription("End-to-end lease latency from LeaseService entry to return.")
                .setUnit("ms")
                .build();

        this.activeLeases = meter
                .upDownCounterBuilder("sandbox.leases.active")
                .setDescription("Sandboxes currently in LEASED state.")
                .setUnit("{sandbox}")
                .build();

        this.watchdogFires = meter
                .counterBuilder("sandbox.watchdog.fires_total")
                .setDescription("Watchdog wall-clock-kill events (sandbox SIGKILLed at deadline).")
                .setUnit("{event}")
                .build();

        this.poolDepth = meter
                .upDownCounterBuilder("sandbox.pool.ready")
                .setDescription("READY-queue depth per language (warm pool).")
                .setUnit("{sandbox}")
                .build();
    }

    // ---------- record helpers ------------------------------------------

    /** Record lease latency for a single lease invocation. Caller stamps
     *  {@code System.nanoTime()} at method entry; we convert to ms here. */
    public void recordLeaseLatency(long elapsedNanos, String language) {
        leaseLatencyMs.record(elapsedNanos / 1_000_000.0, attrs(language));
    }

    /** Transition: PROVISIONING/READY → LEASED. */
    public void incActiveLeases(String language) {
        activeLeases.add(1, attrs(language));
    }

    /** Transition: LEASED → TERMINATED (or any release path that decrements
     *  the active set, including watchdog-fire teardown). */
    public void decActiveLeases(String language) {
        activeLeases.add(-1, attrs(language));
    }

    /**
     * Watchdog wall-clock kill fired. To be called by Workstream CD's
     * {@code WatchdogService.onFire(...)} once the SIGKILL path is in.
     */
    public void incWatchdogFires(String language) {
        watchdogFires.add(1, attrs(language));
    }

    /**
     * Observe the READY-queue depth for a language. To be called by
     * Workstream CD's {@code PoolManager} replenisher every 500ms.
     *
     * <p>Implemented as an up-down counter with delta semantics rather than
     * an async gauge so the caller can compute the delta itself (or pass
     * {@code newDepth - oldDepth}). Async gauges would require us to know
     * every language up-front; the delta approach is allocation-free and
     * stays correct when languages are added dynamically.
     */
    public void observePoolDepth(long delta, String language) {
        poolDepth.add(delta, attrs(language));
    }

    private static Attributes attrs(String language) {
        return Attributes.of(LANGUAGE, language == null ? "unknown" : language);
    }
}
