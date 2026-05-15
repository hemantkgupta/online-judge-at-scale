package com.onlinejudge.worker.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.worker.service.AgentClient.AgentResult;
import com.onlinejudge.worker.service.AgentClient.PerTestResult;
import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AgentClient} (Workstream B).
 *
 * <p>Verifies:
 * <ul>
 *   <li>the JSON wire shape written to the vsock-client's stdin matches the
 *       contract documented in {@link AgentClient}'s class javadoc, AND the
 *       shape the Go agent (Workstream F) expects;</li>
 *   <li>the response parser maps {@code per_test} / {@code overall_verdict}
 *       into {@link AgentResult} + {@link PerTestResult} faithfully.</li>
 * </ul>
 */
class AgentClientTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void exec_serializesNewShape_andParsesPerTestResponse() throws Exception {
        // Capture stdin (what we wrote to oj-vsock-client) and feed it back
        // alongside a hand-crafted response. The fake Process never spawns;
        // we just shuttle bytes.
        ByteArrayOutputStream capturedStdin = new ByteArrayOutputStream();
        String responseJson = "{\"per_test\":["
                + "{\"ordinal\":1,\"verdict\":\"OK\",\"time_ms\":18,\"memory_kb\":2200,\"stdout_hash\":\"abc\"},"
                + "{\"ordinal\":2,\"verdict\":\"WRONG_ANSWER\",\"time_ms\":22,\"memory_kb\":2300,\"stdout_hash\":\"def\"}"
                + "],\"overall_verdict\":\"WRONG_ANSWER\",\"detail\":\"\"}";

        AgentClient client = new AgentClient(objectMapper,
                argv -> new FakeProcess(
                        capturedStdin,
                        responseJson.getBytes(StandardCharsets.UTF_8)));
        ReflectionTestUtils.setField(client, "vsockClientBinary", "/usr/local/bin/oj-vsock-client");

        AgentResult result = client.exec(
                "/tmp/fc-sb-1-vsock.sock", 1234, "tok-1",
                "python", "print(input())", 1000,
                List.of(
                        new TestCaseSpec(1, "1\n", "h1"),
                        new TestCaseSpec(2, "2\n", "h2")));

        // ---- response parsing ----
        assertThat(result.overallVerdict()).isEqualTo("WRONG_ANSWER");
        assertThat(result.perTest()).hasSize(2);
        assertThat(result.perTest().get(0))
                .isEqualTo(new PerTestResult(1, "OK", 18, 2200, "abc"));
        assertThat(result.perTest().get(1))
                .isEqualTo(new PerTestResult(2, "WRONG_ANSWER", 22, 2300, "def"));

        // ---- request shape ----
        String written = capturedStdin.toString(StandardCharsets.UTF_8);
        assertThat(written).endsWith("\n");
        JsonNode n = objectMapper.readTree(written.trim());
        assertThat(n.get("session_token").asText()).isEqualTo("tok-1");
        assertThat(n.get("language").asText()).isEqualTo("python");
        assertThat(n.get("code").asText()).isEqualTo("print(input())");
        assertThat(n.get("time_limit_ms").asInt()).isEqualTo(1000);
        assertThat(n.get("per_test").isArray()).isTrue();
        assertThat(n.get("per_test").size()).isEqualTo(2);
        JsonNode t1 = n.get("per_test").get(0);
        assertThat(t1.get("ordinal").asInt()).isEqualTo(1);
        assertThat(t1.get("input").asText()).isEqualTo("1\n");
        assertThat(t1.get("expected_hash").asText()).isEqualTo("h1");
    }

    @Test
    void exec_emptyTestCases_emitsEmptyArray() throws Exception {
        ByteArrayOutputStream capturedStdin = new ByteArrayOutputStream();
        String responseJson = "{\"per_test\":[],\"overall_verdict\":\"OK\",\"detail\":\"\"}";

        AgentClient client = new AgentClient(objectMapper,
                argv -> new FakeProcess(capturedStdin,
                        responseJson.getBytes(StandardCharsets.UTF_8)));

        AgentResult result = client.exec("/tmp/x", 1, "tok", "python", "code", 500, List.of());

        assertThat(result.overallVerdict()).isEqualTo("OK");
        assertThat(result.perTest()).isEmpty();
        JsonNode n = objectMapper.readTree(capturedStdin.toString(StandardCharsets.UTF_8).trim());
        assertThat(n.get("per_test").isArray()).isTrue();
        assertThat(n.get("per_test").size()).isZero();
    }

    @Test
    void parse_handlesMissingFieldsDefensively() throws Exception {
        AgentClient client = new AgentClient(objectMapper, null);
        AgentResult r = client.parse("{}".getBytes(StandardCharsets.UTF_8));
        assertThat(r.overallVerdict()).isEqualTo("INTERNAL_ERROR");
        assertThat(r.perTest()).isEmpty();
    }

    // ---------- helpers ----------

    /**
     * Hand-rolled fake {@link Process} that swallows stdin, returns canned
     * stdout, and reports immediately-exited status. Cheaper than mocking
     * Process (which has 12+ abstract methods) for this narrow seam.
     */
    private static final class FakeProcess extends Process {
        private final OutputStream stdin;
        private final InputStream stdout;
        private final InputStream stderr = new ByteArrayInputStream(new byte[0]);

        FakeProcess(OutputStream stdin, byte[] stdoutBytes) {
            this.stdin = stdin;
            this.stdout = new ByteArrayInputStream(stdoutBytes);
        }

        @Override public OutputStream getOutputStream() { return stdin; }
        @Override public InputStream getInputStream() { return stdout; }
        @Override public InputStream getErrorStream() { return stderr; }
        @Override public int waitFor() { return 0; }
        @Override public boolean waitFor(long timeout, TimeUnit unit) { return true; }
        @Override public int exitValue() { return 0; }
        @Override public void destroy() {}
        @Override public Process destroyForcibly() { return this; }
    }
}
