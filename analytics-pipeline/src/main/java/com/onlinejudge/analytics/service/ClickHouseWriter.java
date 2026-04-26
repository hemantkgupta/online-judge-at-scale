package com.onlinejudge.analytics.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Batch writer to ClickHouse via HTTP interface.
 *
 * ClickHouse accepts inserts via its HTTP endpoint using native tab-separated format.
 * Batching is critical: ClickHouse is optimized for large batch inserts, not
 * high-frequency small inserts. We buffer records and flush every N records
 * or every M milliseconds, whichever comes first.
 *
 * Table: submission_analytics
 * Engine: MergeTree (local dev; production uses AggregatingMergeTree)
 *
 * Production pattern: Kafka Engine + Materialized View pulls directly from Kafka
 * without a consumer process. Here we use a consumer process + HTTP insert
 * as the simpler local equivalent.
 */
@Slf4j
@Service
public class ClickHouseWriter {

    @Value("${app.clickhouse.url:http://localhost:8123}")
    private String clickHouseUrl;

    @Value("${app.clickhouse.database:onlinejudge}")
    private String database;

    @Value("${app.clickhouse.batch-size:100}")
    private int batchSize;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final List<Map<String, Object>> buffer = new CopyOnWriteArrayList<>();

    public void buffer(Map<String, Object> record) {
        buffer.add(record);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    @Scheduled(fixedDelayString = "${app.clickhouse.flush-interval-ms:5000}")
    public void flushScheduled() {
        if (!buffer.isEmpty()) flush();
    }

    private synchronized void flush() {
        if (buffer.isEmpty()) return;

        List<Map<String, Object>> batch = new ArrayList<>(buffer);
        buffer.clear();

        StringBuilder rows = new StringBuilder();
        for (Map<String, Object> r : batch) {
            rows.append(r.getOrDefault("submissionId", "")).append("\t")
                .append(r.getOrDefault("userId", "")).append("\t")
                .append(r.getOrDefault("problemId", "")).append("\t")
                .append(r.getOrDefault("contestId", "")).append("\t")
                .append(r.getOrDefault("language", "")).append("\t")
                .append(r.getOrDefault("verdict", "")).append("\t")
                .append(r.getOrDefault("executionTimeMs", 0)).append("\t")
                .append(r.getOrDefault("memoryUsedMb", 0)).append("\t")
                .append(Instant.ofEpochMilli(
                    Long.parseLong(r.getOrDefault("eventTsMs", "0").toString())))
                .append("\n");
        }

        String insertQuery = "INSERT INTO " + database + ".submission_analytics "
                + "(submission_id, user_id, problem_id, contest_id, language, verdict, "
                + "execution_time_ms, memory_used_mb, event_time) FORMAT TabSeparated\n"
                + rows;

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(clickHouseUrl))
                    .header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString(insertQuery))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                log.info("[clickhouse] Flushed {} analytics records", batch.size());
            } else {
                log.error("[clickhouse] Insert failed status={} body={}", response.statusCode(), response.body());
                buffer.addAll(batch); // re-buffer for retry
            }
        } catch (Exception ex) {
            log.error("[clickhouse] Flush error: {}", ex.getMessage());
            buffer.addAll(batch); // re-buffer for retry
        }
    }
}
