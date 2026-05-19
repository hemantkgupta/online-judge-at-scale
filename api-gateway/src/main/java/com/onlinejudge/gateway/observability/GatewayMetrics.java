package com.onlinejudge.gateway.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.springframework.stereotype.Component;

/**
 * Api-gateway custom metrics (tech-spec §9.3).
 *
 * <p>Mirrors {@code com.onlinejudge.worker.observability.WorkerMetrics} and
 * {@code com.onlinejudge.sandbox.observability.SandboxMetrics} — see those
 * classes for the operating model. The OTel Java agent loaded via
 * {@code JAVA_TOOL_OPTIONS} (see {@code api-gateway/Dockerfile}) provides
 * the runtime SDK. When {@code OTEL_EXPORTER_OTLP_ENDPOINT} is unset the
 * agent is a no-op and {@link GlobalOpenTelemetry#getMeter} returns a
 * no-op meter — measurements are silently dropped.
 *
 * <h2>Wire-in points</h2>
 * <ul>
 *   <li><b>{@link #incSubmissionAccepted}</b> — wired in
 *       {@code SubmissionService.accept(...)} immediately after the outbox
 *       row is persisted (i.e. after the durability invariant has cleared,
 *       not before).</li>
 *   <li><b>{@link #incSubmissionRejectedSizeTooLarge}</b> — wired in
 *       {@code SubmissionController.submit(...)}'s Content-Length 413
 *       branch (Roadmap §3.6 cap).</li>
 *   <li><b>{@link #addReconciliationSwept} /
 *       {@link #addReconciliationRepublished} /
 *       {@link #addReconciliationDlq}</b> — wired in
 *       {@code ReconciliationScanner.sweep()} after each per-row outcome
 *       (or in bulk at sweep end, as long as the deltas match the
 *       in-memory ScanResult counters).</li>
 * </ul>
 *
 * <h2>Metric inventory</h2>
 * <pre>
 *   submission.accepted                      (counter)   attrs: region
 *   submission.rejected.size_too_large       (counter)   attrs: region
 *   gateway.reconciliation.swept_total       (counter)   attrs: (none)
 *   gateway.reconciliation.republished_total (counter)   attrs: (none)
 *   gateway.reconciliation.dlq_total         (counter)   attrs: (none)
 * </pre>
 *
 * <p>Naming follows the rows in tech-spec §9.3 verbatim — the metric
 * names are operator-facing and tied to dashboard panels under
 * {@code infra/observability/dashboards/}.
 */
@Component
public class GatewayMetrics {

    public static final AttributeKey<String> REGION = AttributeKey.stringKey("region");

    private static final String SCOPE = "com.onlinejudge.gateway";

    private final LongCounter submissionAccepted;
    private final LongCounter submissionRejectedSizeTooLarge;
    private final LongCounter reconciliationSwept;
    private final LongCounter reconciliationRepublished;
    private final LongCounter reconciliationDlq;

    public GatewayMetrics() {
        Meter meter = GlobalOpenTelemetry.getMeter(SCOPE);

        this.submissionAccepted = meter
                .counterBuilder("submission.accepted")
                .setDescription("Submissions accepted by the gateway (post-durability, pre-Kafka).")
                .setUnit("{submission}")
                .build();

        this.submissionRejectedSizeTooLarge = meter
                .counterBuilder("submission.rejected.size_too_large")
                .setDescription("Submissions rejected because the request body exceeded the gateway cap.")
                .setUnit("{submission}")
                .build();

        // Three separate counters rather than a single counter with a
        // {swept,republished,dlq} label — the §9.3 row spells out the
        // _total suffix per outcome, and dashboards alert on each one
        // independently (republished is healthy; dlq is alarm-worthy).
        this.reconciliationSwept = meter
                .counterBuilder("gateway.reconciliation.swept_total")
                .setDescription("Stuck submissions surfaced by one reconciliation sweep.")
                .setUnit("{row}")
                .build();

        this.reconciliationRepublished = meter
                .counterBuilder("gateway.reconciliation.republished_total")
                .setDescription("Stuck submissions successfully re-published to the pretest topic.")
                .setUnit("{row}")
                .build();

        this.reconciliationDlq = meter
                .counterBuilder("gateway.reconciliation.dlq_total")
                .setDescription("Stuck submissions routed to the DLQ after exhausting reconciliation attempts.")
                .setUnit("{row}")
                .build();
    }

    /** Submission was durably accepted (submissions row + outbox row written). */
    public void incSubmissionAccepted(String region) {
        submissionAccepted.add(1, regionAttrs(region));
    }

    /**
     * Submission was rejected at the Content-Length stage for exceeding
     * the gateway body cap (Roadmap §3.6). Wired in {@code SubmissionController}.
     */
    public void incSubmissionRejectedSizeTooLarge(String region) {
        submissionRejectedSizeTooLarge.add(1, regionAttrs(region));
    }

    /** Roadmap §3.9: increment by the number of rows the scanner found stuck. */
    public void addReconciliationSwept(long delta) {
        if (delta > 0) {
            reconciliationSwept.add(delta);
        }
    }

    /** Roadmap §3.9: increment by the number of rows successfully republished. */
    public void addReconciliationRepublished(long delta) {
        if (delta > 0) {
            reconciliationRepublished.add(delta);
        }
    }

    /** Roadmap §3.9: increment by the number of rows routed to DLQ. */
    public void addReconciliationDlq(long delta) {
        if (delta > 0) {
            reconciliationDlq.add(delta);
        }
    }

    private static Attributes regionAttrs(String region) {
        return Attributes.of(REGION, region == null || region.isBlank() ? "unknown" : region);
    }
}
