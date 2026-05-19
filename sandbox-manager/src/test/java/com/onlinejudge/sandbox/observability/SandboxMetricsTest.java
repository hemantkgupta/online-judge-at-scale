package com.onlinejudge.sandbox.observability;

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
 * Unit tests for {@link SandboxMetrics} (tech-spec §9.3).
 *
 * <p>The class predates this commit but the §9.3 row catalogue was never
 * locked in by tests — this fills that gap so we can mark the rows
 * "implemented" with the file/line in the spec.
 */
class SandboxMetricsTest {

    private InMemoryMetricReader reader;
    private SandboxMetrics metrics;

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
        metrics = new SandboxMetrics();
    }

    @AfterEach
    void tearDown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void leaseLatency_recorded_per_language() {
        metrics.recordLeaseLatency(2_000_000L, "python");   // 2 ms
        metrics.recordLeaseLatency(5_000_000L, "python");   // 5 ms
        metrics.recordLeaseLatency(8_000_000L, "cpp");      // 8 ms

        long pythonCount = histogramCountFor("sandbox.lease.latency_ms", "python");
        long cppCount    = histogramCountFor("sandbox.lease.latency_ms", "cpp");
        assertThat(pythonCount).isEqualTo(2L);
        assertThat(cppCount).isEqualTo(1L);
    }

    @Test
    void activeLeases_increments_and_decrements_per_language() {
        metrics.incActiveLeases("python");
        metrics.incActiveLeases("python");
        metrics.incActiveLeases("cpp");
        metrics.decActiveLeases("python");

        long pythonActive = updownValueFor("sandbox.leases.active", "python");
        long cppActive    = updownValueFor("sandbox.leases.active", "cpp");
        assertThat(pythonActive).isEqualTo(1L);
        assertThat(cppActive).isEqualTo(1L);
    }

    @Test
    void poolDepth_observes_delta_per_language() {
        metrics.observePoolDepth(8, "python");
        metrics.observePoolDepth(-3, "python");
        metrics.observePoolDepth(4, "cpp");

        long pythonDepth = updownValueFor("sandbox.pool.ready", "python");
        long cppDepth    = updownValueFor("sandbox.pool.ready", "cpp");
        assertThat(pythonDepth).isEqualTo(5L);
        assertThat(cppDepth).isEqualTo(4L);
    }

    // ----- helpers ------------------------------------------------------

    private long histogramCountFor(String metricName, String language) {
        Collection<MetricData> all = reader.collectAllMetrics();
        return all.stream()
                .filter(m -> m.getName().equals(metricName))
                .flatMap(m -> m.getHistogramData().getPoints().stream())
                .filter(p -> language.equals(p.getAttributes().get(SandboxMetrics.LANGUAGE)))
                .mapToLong(p -> p.getCount())
                .sum();
    }

    private long updownValueFor(String metricName, String language) {
        Collection<MetricData> all = reader.collectAllMetrics();
        return all.stream()
                .filter(m -> m.getName().equals(metricName))
                .flatMap(m -> m.getLongSumData().getPoints().stream())
                .filter(p -> language.equals(p.getAttributes().get(SandboxMetrics.LANGUAGE)))
                .mapToLong(p -> p.getValue())
                .sum();
    }
}
