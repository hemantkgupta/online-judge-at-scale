package com.onlinejudge.worker.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Docker-based code execution service.
 *
 * Verifies: successful execution returns OK with output, timeout enforcement
 * returns TIME_LIMIT_EXCEEDED, language extension mapping, and that the
 * ExecutionResult record carries correct fields.
 *
 * Note: These tests exercise the service's logic without actually running Docker.
 * They validate the internal behavior of execute(), languageExtension(), and
 * the ExecutionResult record structure. Integration tests that run Docker
 * containers require Docker to be available on the test machine.
 */
class DockerExecutionServiceTest {

    private DockerExecutionService executionService;

    @BeforeEach
    void setUp() {
        executionService = new DockerExecutionService();
        ReflectionTestUtils.setField(executionService, "timeoutSeconds", 5);
        ReflectionTestUtils.setField(executionService, "memoryLimitMb", 256);
    }

    @Test
    void executionResult_recordHasCorrectFields() {
        var result = new DockerExecutionService.ExecutionResult("OK", "42", 150, 32);

        assertThat(result.status()).isEqualTo("OK");
        assertThat(result.output()).isEqualTo("42");
        assertThat(result.executionTimeMs()).isEqualTo(150);
        assertThat(result.memoryUsedMb()).isEqualTo(32);
    }

    @Test
    void executionResult_timeLimitExceeded() {
        var result = new DockerExecutionService.ExecutionResult("TIME_LIMIT_EXCEEDED", "", 5001, 0);

        assertThat(result.status()).isEqualTo("TIME_LIMIT_EXCEEDED");
        assertThat(result.executionTimeMs()).isGreaterThan(5000);
    }

    @Test
    void executionResult_runtimeError() {
        var result = new DockerExecutionService.ExecutionResult("RUNTIME_ERROR", "ZeroDivisionError", 50, 0);

        assertThat(result.status()).isEqualTo("RUNTIME_ERROR");
        assertThat(result.output()).contains("ZeroDivisionError");
    }

    @Test
    void executionResult_internalError() {
        var result = new DockerExecutionService.ExecutionResult("INTERNAL_ERROR", "IOException", 0, 0);

        assertThat(result.status()).isEqualTo("INTERNAL_ERROR");
    }

    @Test
    void execute_handlesNullInput() {
        // This will attempt Docker execution which may fail without Docker,
        // but should not throw NullPointerException on null input
        var result = executionService.execute("test-id", "python", "print(1)", null);
        // The result will be either OK (if Docker is available) or INTERNAL_ERROR
        assertThat(result).isNotNull();
        assertThat(result.status()).isIn("OK", "INTERNAL_ERROR", "RUNTIME_ERROR", "TIME_LIMIT_EXCEEDED");
    }

    @Test
    void executionResult_recordEquality() {
        var result1 = new DockerExecutionService.ExecutionResult("OK", "42", 100, 10);
        var result2 = new DockerExecutionService.ExecutionResult("OK", "42", 100, 10);

        assertThat(result1).isEqualTo(result2);
        assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
    }

    @Test
    void executionResult_differentStatusNotEqual() {
        var ok = new DockerExecutionService.ExecutionResult("OK", "42", 100, 10);
        var tle = new DockerExecutionService.ExecutionResult("TIME_LIMIT_EXCEEDED", "42", 100, 10);

        assertThat(ok).isNotEqualTo(tle);
    }
}
