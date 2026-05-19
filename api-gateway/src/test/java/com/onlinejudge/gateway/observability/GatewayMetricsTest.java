package com.onlinejudge.gateway.observability;

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
 * Unit tests for {@link GatewayMetrics} (tech-spec §9.3).
 *
 * <p>Drives the OTel SDK in-memory: the test installs an
 * {@link InMemoryMetricReader} into {@link GlobalOpenTelemetry} BEFORE
 * instantiating the metrics class, so the cached {@code Meter} references
 * are tied to our test provider. After each assertion we
 * {@link GlobalOpenTelemetry#resetForTest()} so the next test starts from
 * a clean slate (the singleton would otherwise throw on a second
 * {@code set()} call within the same JVM).
 */
class GatewayMetricsTest {

    private InMemoryMetricReader reader;
    private GatewayMetrics metrics;

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
        metrics = new GatewayMetrics();
    }

    @AfterEach
    void tearDown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void incSubmissionAccepted_increments_with_region_attribute() {
        metrics.incSubmissionAccepted("asia-south1");
        metrics.incSubmissionAccepted("asia-south1");
        metrics.incSubmissionAccepted("us-central1");

        long asia = sumPointsFor("submission.accepted", "asia-south1");
        long us   = sumPointsFor("submission.accepted", "us-central1");
        assertThat(asia).isEqualTo(2L);
        assertThat(us).isEqualTo(1L);
    }

    @Test
    void incSubmissionRejectedSizeTooLarge_increments_with_region_attribute() {
        metrics.incSubmissionRejectedSizeTooLarge("asia-south1");

        long asia = sumPointsFor("submission.rejected.size_too_large", "asia-south1");
        assertThat(asia).isEqualTo(1L);
    }

    @Test
    void incSubmissionAccepted_with_null_region_falls_back_to_unknown() {
        metrics.incSubmissionAccepted(null);
        metrics.incSubmissionAccepted("");

        long unknown = sumPointsFor("submission.accepted", "unknown");
        assertThat(unknown).isEqualTo(2L);
    }

    @Test
    void reconciliation_counters_advance_independently() {
        metrics.addReconciliationSwept(5);
        metrics.addReconciliationRepublished(3);
        metrics.addReconciliationDlq(1);

        assertThat(totalPointsFor("gateway.reconciliation.swept_total")).isEqualTo(5L);
        assertThat(totalPointsFor("gateway.reconciliation.republished_total")).isEqualTo(3L);
        assertThat(totalPointsFor("gateway.reconciliation.dlq_total")).isEqualTo(1L);
    }

    @Test
    void reconciliation_counters_ignore_zero_and_negative_deltas() {
        metrics.addReconciliationSwept(0);
        metrics.addReconciliationSwept(-7);
        metrics.addReconciliationSwept(2);

        // Only the +2 should count.
        assertThat(totalPointsFor("gateway.reconciliation.swept_total")).isEqualTo(2L);
    }

    // ----- helpers ------------------------------------------------------

    private long sumPointsFor(String metricName, String regionAttr) {
        Collection<MetricData> all = reader.collectAllMetrics();
        return all.stream()
                .filter(m -> m.getName().equals(metricName))
                .flatMap(m -> m.getLongSumData().getPoints().stream())
                .filter(p -> regionAttr.equals(p.getAttributes().get(GatewayMetrics.REGION)))
                .mapToLong(p -> p.getValue())
                .sum();
    }

    private long totalPointsFor(String metricName) {
        Collection<MetricData> all = reader.collectAllMetrics();
        return all.stream()
                .filter(m -> m.getName().equals(metricName))
                .flatMap(m -> m.getLongSumData().getPoints().stream())
                .mapToLong(p -> p.getValue())
                .sum();
    }
}
