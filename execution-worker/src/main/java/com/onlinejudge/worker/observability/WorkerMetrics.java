package com.onlinejudge.worker.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Execution-worker custom metrics (Workstream H).
 *
 * <p>Mirrors {@code com.onlinejudge.sandbox.observability.SandboxMetrics} —
 * see that class for the operating model. The OTel Java agent provides
 * the runtime SDK (loaded via {@code JAVA_TOOL_OPTIONS} from
 * {@code execution-worker/Dockerfile}); when
 * {@code OTEL_EXPORTER_OTLP_ENDPOINT} is unset the agent is a no-op.
 *
 * <h2>Wire-in points (other workstreams)</h2>
 * <ul>
 *   <li><b>{@link #recordGcsFetchLatency}</b> — to be wired by Workstream B
 *       inside {@code GcsClient.fetch(...)} around the HTTP GET. <i>That
 *       class does not exist yet; this metric is a stub until B lands.</i></li>
 *   <li><b>{@link #recordAgentExecLatency}</b> — to be wired by Workstream E
 *       inside {@code AgentClient.exec(...)} around the vsock-client
 *       ProcessBuilder.start/waitFor pair. <i>Owned by E — DO NOT edit
 *       that file from this workstream.</i></li>
 *   <li><b>{@link #incVerdictsPublished}</b> — wired by Workstream H in
 *       {@code SubmissionConsumer} after the {@code evaluated_results}
 *       Kafka send returns. Verdict label is attached.</li>
 * </ul>
 *
 * <h2>Metric inventory</h2>
 * <pre>
 *   worker.gcs.fetch.latency_ms        (histogram)   attrs: bucket
 *   worker.agent.exec.latency_ms       (histogram)   attrs: language
 *   worker.verdicts.published_total    (counter)     attrs: verdict, language, phase
 *   worker.idempotency.attempts_max    (async gauge) attrs: (none) — high-water mark
 * </pre>
 *
 * <p><b>idempotency.attempts_max</b> mirrors tech-spec §9.3. The
 * idempotency row's {@code attempts} column is bumped each time a stale
 * claim is reclaimed; this gauge tracks the highest value observed across
 * the worker's lifetime, surfacing the worst-case reclaim depth without
 * having to scan the whole table. Reset on JVM restart by design — the
 * dashboard alert is "max ever observed since boot is approaching the
 * cap configured in app.idempotency.max-attempts".
 */
@Component
public class WorkerMetrics {

    public static final AttributeKey<String> LANGUAGE = AttributeKey.stringKey("language");
    public static final AttributeKey<String> VERDICT  = AttributeKey.stringKey("verdict");
    public static final AttributeKey<String> PHASE    = AttributeKey.stringKey("phase");
    public static final AttributeKey<String> BUCKET   = AttributeKey.stringKey("bucket");

    private static final String SCOPE = "com.onlinejudge.worker";

    private final DoubleHistogram gcsFetchLatencyMs;
    private final DoubleHistogram agentExecLatencyMs;
    private final LongCounter verdictsPublished;
    /**
     * Tech-spec §9.3: high-water mark of the idempotency {@code attempts}
     * counter observed at the worker. Backed by an {@link AtomicLong} so
     * the async-gauge callback reads a consistent snapshot from any
     * thread. Kept as a field (not a closure-captured local) so the
     * unit-test fixture can reach it via reflection-free getters.
     */
    private final AtomicLong idempotencyAttemptsMax = new AtomicLong(0);

    public WorkerMetrics() {
        Meter meter = GlobalOpenTelemetry.getMeter(SCOPE);

        this.gcsFetchLatencyMs = meter
                .histogramBuilder("worker.gcs.fetch.latency_ms")
                .setDescription("Latency of a single GCS object GET (source code or test case).")
                .setUnit("ms")
                .build();

        this.agentExecLatencyMs = meter
                .histogramBuilder("worker.agent.exec.latency_ms")
                .setDescription("Latency of the AgentClient.exec call against a leased sandbox.")
                .setUnit("ms")
                .build();

        this.verdictsPublished = meter
                .counterBuilder("worker.verdicts.published_total")
                .setDescription("Verdicts written to the evaluated_results Kafka topic.")
                .setUnit("{verdict}")
                .build();

        // Async gauge: the OTel SDK invokes the callback at every collection
        // tick (default 60s with the agent SDK). Using an async gauge instead
        // of an up-down counter so the value is monotonically non-decreasing
        // — we want a high-water mark, not a delta-applied running total.
        meter.gaugeBuilder("worker.idempotency.attempts_max")
                .ofLongs()
                .setDescription("Highest idempotency attempts counter observed since worker boot.")
                .setUnit("{attempt}")
                .buildWithCallback(measurement ->
                        measurement.record(idempotencyAttemptsMax.get()));
    }

    /** Workstream B wire-in. Bucket is "source" or "tests". */
    public void recordGcsFetchLatency(long elapsedNanos, String bucket) {
        gcsFetchLatencyMs.record(elapsedNanos / 1_000_000.0,
                Attributes.of(BUCKET, bucket == null ? "unknown" : bucket));
    }

    /** Workstream E wire-in. */
    public void recordAgentExecLatency(long elapsedNanos, String language) {
        agentExecLatencyMs.record(elapsedNanos / 1_000_000.0,
                Attributes.of(LANGUAGE, language == null ? "unknown" : language));
    }

    /**
     * Workstream H wire-in (this workstream): increment whenever a
     * {@code VerdictEvent} successfully lands on the {@code evaluated_results}
     * topic.
     */
    public void incVerdictsPublished(String verdict, String language, String phase) {
        verdictsPublished.add(1, Attributes.of(
                VERDICT,  verdict  == null ? "UNKNOWN" : verdict,
                LANGUAGE, language == null ? "unknown" : language,
                PHASE,    phase    == null ? "unknown" : phase));
    }

    /**
     * Tech-spec §9.3 wire-in: every successful idempotency claim reports
     * the {@code attempts} value of the row that won the race (1 on the
     * first try, n on the n-th reclaim of a stale row). The gauge keeps
     * the max via lock-free CAS. Non-positive inputs are silently dropped
     * — they're a no-op for a max.
     */
    public void observeIdempotencyAttempts(long attempts) {
        if (attempts <= 0) {
            return;
        }
        long current;
        do {
            current = idempotencyAttemptsMax.get();
            if (attempts <= current) {
                return;
            }
        } while (!idempotencyAttemptsMax.compareAndSet(current, attempts));
    }

    /** Test-only accessor. Returns the current high-water mark. */
    public long currentIdempotencyAttemptsMax() {
        return idempotencyAttemptsMax.get();
    }
}
