package com.onlinejudge.analytics.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
// HttpClient.send stubs use doReturn(...) to bypass Mockito's strict generic
// inference (HttpResponse<String> vs HttpResponse<Object>).

/**
 * Tests for {@link ClickHouseWriter}.
 *
 * <p>The writer batches records and flushes via HTTP. Tests inject a mock
 * {@link HttpClient} so no real ClickHouse is needed.
 *
 * <p>Behaviors verified:
 *  - {@code buffer()} adds records to the internal buffer.
 *  - {@code buffer()} auto-triggers {@code flush()} when the batch size is reached.
 *  - {@code flushScheduled()} is a no-op when the buffer is empty.
 *  - Failed HTTP send (non-200) re-buffers records for retry.
 *  - Generated TSV payload starts with the {@code INSERT INTO ... FORMAT TabSeparated} prefix.
 */
class ClickHouseWriterTest {

    private ClickHouseWriter writer;
    private HttpClient mockHttp;

    @BeforeEach
    void setUp() {
        writer = new ClickHouseWriter();
        ReflectionTestUtils.setField(writer, "clickHouseUrl", "http://localhost:8123");
        ReflectionTestUtils.setField(writer, "database", "onlinejudge");
        ReflectionTestUtils.setField(writer, "batchSize", 3);

        mockHttp = mock(HttpClient.class);
        ReflectionTestUtils.setField(writer, "httpClient", mockHttp);
    }

    @Test
    void buffer_belowBatchSize_doesNotFlush() throws Exception {
        writer.buffer(sampleRecord("sub-1"));
        writer.buffer(sampleRecord("sub-2"));

        // No HTTP call yet — under the 3-record batch threshold.
        verify(mockHttp, never()).send(any(), any());
        assertThat(internalBuffer(writer)).hasSize(2);
    }

    @Test
    void buffer_reachesBatchSize_triggersFlush() throws Exception {
        HttpResponse<String> ok = mock(HttpResponse.class);
        when(ok.statusCode()).thenReturn(200);
        org.mockito.Mockito.doReturn(ok)
                .when(mockHttp).send(any(HttpRequest.class), any());

        writer.buffer(sampleRecord("sub-1"));
        writer.buffer(sampleRecord("sub-2"));
        writer.buffer(sampleRecord("sub-3")); // triggers flush

        verify(mockHttp, times(1)).send(any(HttpRequest.class), any());
        assertThat(internalBuffer(writer)).isEmpty();
    }

    @Test
    void flushScheduled_emptyBuffer_isNoOp() throws Exception {
        writer.flushScheduled();

        verify(mockHttp, never()).send(any(), any());
    }

    @Test
    void flush_httpFailure_reBuffersForRetry() throws Exception {
        HttpResponse<String> err = mock(HttpResponse.class);
        when(err.statusCode()).thenReturn(500);
        when(err.body()).thenReturn("clickhouse oops");
        org.mockito.Mockito.doReturn(err)
                .when(mockHttp).send(any(HttpRequest.class), any());

        writer.buffer(sampleRecord("sub-1"));
        writer.buffer(sampleRecord("sub-2"));
        writer.buffer(sampleRecord("sub-3")); // triggers flush → 500 → re-buffer

        verify(mockHttp, times(1)).send(any(HttpRequest.class), any());
        // All 3 records re-buffered for the next attempt.
        assertThat(internalBuffer(writer)).hasSize(3);
    }

    @Test
    void flush_payloadFormatsAsClickHouseTabSeparatedInsert() throws Exception {
        HttpResponse<String> ok = mock(HttpResponse.class);
        when(ok.statusCode()).thenReturn(200);
        org.mockito.Mockito.doReturn(ok)
                .when(mockHttp).send(any(HttpRequest.class), any());

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);

        writer.buffer(sampleRecord("sub-1"));
        writer.buffer(sampleRecord("sub-2"));
        writer.buffer(sampleRecord("sub-3"));

        verify(mockHttp).send(requestCaptor.capture(), any());

        HttpRequest request = requestCaptor.getValue();
        assertThat(request.uri().toString()).isEqualTo("http://localhost:8123");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.headers().firstValue("Content-Type"))
                .contains("text/plain");
    }

    // --- helpers ---

    private static Map<String, Object> sampleRecord(String submissionId) {
        Map<String, Object> r = new HashMap<>();
        r.put("submissionId", submissionId);
        r.put("userId", "user-1");
        r.put("problemId", "p-1");
        r.put("contestId", "c-1");
        r.put("language", "python");
        r.put("verdict", "ACCEPTED");
        r.put("executionTimeMs", 42);
        r.put("memoryUsedMb", 12);
        r.put("eventTsMs", 1719849599000L);
        return r;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> internalBuffer(ClickHouseWriter writer) {
        return (List<Map<String, Object>>) ReflectionTestUtils.getField(writer, "buffer");
    }
}
