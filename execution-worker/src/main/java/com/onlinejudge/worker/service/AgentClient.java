package com.onlinejudge.worker.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Host-side dialer for the in-guest Execution Agent.
 *
 * <p>Per Workstream B's Option-α pivot, the worker sends code + per-test
 * inputs + expected hashes directly to the agent over vsock — no mounted
 * code drive on the warm-pool VM. The Sandbox Manager stays on the
 * lease/release control plane only, matching Part 7's three-zone split.
 *
 * <p>The JVM has no native AF_VSOCK. Firecracker exposes vsock to the host
 * as a Unix-domain socket with a tiny CONNECT handshake on top — we shell
 * out to the same Go bridge binary the SM uses ({@code oj-vsock-client}),
 * baked into the worker image.
 *
 * <h2>Wire format (worker → agent)</h2>
 * <pre>
 * {
 *   "session_token": "&lt;uuid&gt;",
 *   "language": "python|java|cpp",
 *   "code": "&lt;source&gt;",
 *   "time_limit_ms": 1000,
 *   "per_test": [
 *     {"ordinal": 1, "input": "&lt;stdin&gt;", "expected_hash": "&lt;sha256&gt;"},
 *     ...
 *   ]
 * }
 * </pre>
 *
 * <p>The agent hashes its actual stdout with the same canonicalization rule
 * ({@link TestCaseFetcher#canonicalHash(byte[])}) and compares against
 * {@code expected_hash} — the worker never ships plaintext expected stdout,
 * keeping the contest's test cases secret even from the in-guest agent.
 *
 * <h2>Wire format (agent → worker)</h2>
 * <pre>
 * {
 *   "per_test": [
 *     {"ordinal": 1, "verdict": "OK", "time_ms": 18,
 *      "memory_kb": 2200, "stdout_hash": "..."},
 *     ...
 *   ],
 *   "overall_verdict": "OK|WRONG_ANSWER|...",
 *   "detail": ""
 * }
 * </pre>
 */
@Slf4j
@Component
public class AgentClient {

    @Value("${app.agent.vsock-client:/usr/local/bin/oj-vsock-client}")
    private String vsockClientBinary;

    private final ObjectMapper objectMapper;
    /** Test-only seam: lets unit tests substitute the subprocess. */
    private final Function<List<String>, Process> processFactory;

    public AgentClient(ObjectMapper objectMapper) {
        this(objectMapper, null);
    }

    /** Test-friendly ctor — pass a process factory to bypass ProcessBuilder. */
    AgentClient(ObjectMapper objectMapper, Function<List<String>, Process> processFactory) {
        this.objectMapper = objectMapper;
        this.processFactory = processFactory;
    }

    /**
     * Run all test cases for a leased sandbox.
     *
     * @param vsockUdsPath path to the Unix-domain socket exposing vsock
     * @param vsockPort    port the in-guest agent listens on
     * @param sessionToken UUID the Sandbox Manager stamped on the lease
     * @param language     {@code python|java|cpp}
     * @param code         contestant source as a UTF-8 string
     * @param timeoutMs    per-test execution time limit
     * @param testCases    ordered per-test inputs + expected SHA-256 hashes
     */
    public AgentResult exec(String vsockUdsPath, int vsockPort, String sessionToken,
                            String language, String code, int timeoutMs,
                            List<TestCaseSpec> testCases) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("session_token", sessionToken == null ? "" : sessionToken);
        req.put("language",      language);
        req.put("code",          code == null ? "" : code);
        req.put("time_limit_ms", timeoutMs <= 0 ? 5000 : timeoutMs);

        List<Map<String, Object>> perTest = new ArrayList<>();
        if (testCases != null) {
            for (TestCaseSpec t : testCases) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("ordinal",       t.ordinal());
                row.put("input",         t.input() == null ? "" : t.input());
                row.put("expected_hash", t.expectedHash() == null ? "" : t.expectedHash());
                perTest.add(row);
            }
        }
        req.put("per_test", perTest);

        return invoke(vsockUdsPath, vsockPort, req, timeoutMs);
    }

    private AgentResult invoke(String vsockUdsPath, int vsockPort,
                               Map<String, Object> req, int timeoutMs) {
        try {
            byte[] reqBytes = (objectMapper.writeValueAsString(req) + "\n")
                    .getBytes(StandardCharsets.UTF_8);

            List<String> argv = List.of(
                    vsockClientBinary == null ? "" : vsockClientBinary,
                    vsockUdsPath == null ? "" : vsockUdsPath,
                    String.valueOf(vsockPort));
            Process p = processFactory != null
                    ? processFactory.apply(argv)
                    : new ProcessBuilder(argv).redirectErrorStream(false).start();

            try (OutputStream stdin = p.getOutputStream()) {
                stdin.write(reqBytes);
                stdin.flush();
            }

            // Generous headroom: contestant timeout + compile + spawn + vsock IO.
            int waitSeconds = (timeoutMs / 1000) + 30;
            if (!p.waitFor(waitSeconds, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return AgentResult.internalError("vsock-client wait timed out");
            }

            byte[] respBytes = p.getInputStream().readAllBytes();
            if (respBytes.length == 0) {
                byte[] err = p.getErrorStream().readAllBytes();
                return AgentResult.internalError(
                        "empty response from agent: " + new String(err, StandardCharsets.UTF_8));
            }
            return parse(respBytes);

        } catch (IOException | InterruptedException ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("[worker] agent exec failed uds={}: {}", vsockUdsPath, ex.getMessage());
            return AgentResult.internalError(ex.getMessage());
        }
    }

    AgentResult parse(byte[] respBytes) throws IOException {
        JsonNode n = objectMapper.readTree(respBytes);

        String overall = n.path("overall_verdict").asText(
                n.path("verdict").asText("INTERNAL_ERROR"));
        String detail  = n.path("detail").asText("");

        List<PerTestResult> perTest = new ArrayList<>();
        JsonNode pt = n.path("per_test");
        if (pt.isArray()) {
            for (JsonNode t : pt) {
                perTest.add(new PerTestResult(
                        t.path("ordinal").asInt(0),
                        t.path("verdict").asText("INTERNAL_ERROR"),
                        t.path("time_ms").asLong(0),
                        t.path("memory_kb").asLong(0),
                        t.path("stdout_hash").asText("")));
            }
        }
        return new AgentResult(overall, detail, perTest);
    }

    /** One per-test row in the agent response. */
    public record PerTestResult(int ordinal, String verdict,
                                long timeMs, long memoryKb, String stdoutHash) {}

    /**
     * Worker-facing response.
     *
     * <p>{@code overallVerdict} is {@code OK} only if every per-test verdict is
     * {@code OK}; otherwise it is the first non-OK verdict (the agent computes
     * this, the worker takes it as the authority).
     */
    public record AgentResult(String overallVerdict, String detail,
                              List<PerTestResult> perTest) {
        public static AgentResult internalError(String detail) {
            return new AgentResult("INTERNAL_ERROR",
                    detail == null ? "" : detail, List.of());
        }
    }
}
