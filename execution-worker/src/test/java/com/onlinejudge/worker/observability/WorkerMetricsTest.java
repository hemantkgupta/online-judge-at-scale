package com.onlinejudge.worker.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WorkerMetrics} (tech-spec §9.3).
 *
 * <p>Specifically covers the {@code worker.idempotency.attempts_max}
 * async-gauge wire-in added alongside the existing
 * {@code worker.verdicts.published_total} counter.
 */
class WorkerMetricsTest {

    private InMemoryMetricReader reader;
    private WorkerMetrics metrics;

    @BeforeEach
    void setUp() {
        GlobalOpenTelemetry.resetForTest();
        reader = InMemoryMetricReader.create();
        SdkMeterProvider provider = SdkMeterProvider.builder()
                .registerMetricReader(reader)
                .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(provider)
                .build();
        GlobalOpenTelemetry.set(sdk);
        metrics = new WorkerMetrics();
    }

    @AfterEach
    void tearDown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void incVerdictsPublished_increments_with_verdict_language_phase_attrs() {
        metrics.incVerdictsPublished("ACCEPTED", "python", "pretest");
        metrics.incVerdictsPublished("ACCEPTED", "python", "pretest");
        metrics.incVerdictsPublished("WRONG_ANSWER", "python", "pretest");

        long accepted = sumPointsFor("worker.verdicts.published_total",
                "ACCEPTED", "python", "pretest");
        long wa = sumPointsFor("worker.verdicts.published_total",
                "WRONG_ANSWER", "python", "pretest");
        assertThat(accepted).isEqualTo(2L);
        assertThat(wa).isEqualTo(1L);
    }

    @Test
    void idempotencyAttemptsMax_tracks_high_water_mark() {
        metrics.observeIdempotencyAttempts(1);
        metrics.observeIdempotencyAttempts(3);
        // Lower values must not regress the gauge.
        metrics.observeIdempotencyAttempts(2);

        long observed = gaugeValueFor("worker.idempotency.attempts_max");
        assertThat(observed).isEqualTo(3L);
        // And the in-memory accessor matches what the gauge reported.
        assertThat(metrics.currentIdempotencyAttemptsMax()).isEqualTo(3L);
    }

    @Test
    void idempotencyAttemptsMax_ignores_zero_and_negative() {
        metrics.observeIdempotencyAttempts(0);
        metrics.observeIdempotencyAttempts(-1);

        // Async gauge with no positive observation still reports 0 — the
        // callback unconditionally records currentIdempotencyAttemptsMax.
        long observed = gaugeValueFor("worker.idempotency.attempts_max");
        assertThat(observed).isEqualTo(0L);
    }

    // ----- helpers ------------------------------------------------------

    private long sumPointsFor(String metricName, String verdict, String language, String phase) {
        Collection<MetricData> all = reader.collectAllMetrics();
        return all.stream()
                .filter(m -> m.getName().equals(metricName))
                .flatMap(m -> m.getLongSumData().getPoints().stream())
                .filter(p -> verdict.equals(p.getAttributes().get(WorkerMetrics.VERDICT))
                        && language.equals(p.getAttributes().get(WorkerMetrics.LANGUAGE))
                        && phase.equals(p.getAttributes().get(WorkerMetrics.PHASE)))
                .mapToLong(p -> p.getValue())
                .sum();
    }

    private long gaugeValueFor(String metricName) {
        Collection<MetricData> all = reader.collectAllMetrics();
        return all.stream()
                .filter(m -> m.getName().equals(metricName))
                .flatMap(m -> m.getLongGaugeData().getPoints().stream())
                .mapToLong(p -> p.getValue())
                .findFirst()
                .orElse(-1L);
    }
}
