package com.onlinejudge.worker.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.springframework.stereotype.Component;

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
 *   worker.gcs.fetch.latency_ms   (histogram)   attrs: bucket
 *   worker.agent.exec.latency_ms  (histogram)   attrs: language
 *   worker.verdicts.published_total (counter)   attrs: verdict, language, phase
 * </pre>
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
}
