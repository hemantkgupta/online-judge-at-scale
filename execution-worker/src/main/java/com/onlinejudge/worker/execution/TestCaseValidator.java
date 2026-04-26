package com.onlinejudge.worker.execution;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Validates execution output against expected test case results.
 *
 * Supports multiple comparison modes:
 *
 * 1. EXACT: output must match expected exactly (after whitespace normalization)
 * 2. TOKEN: output is split into tokens; token sequence must match
 * 3. FLOAT_TOLERANCE: for floating point problems, tokens are compared with epsilon
 *
 * Whitespace normalization rules (same as Codeforces/DMOJ):
 * - Leading/trailing whitespace is stripped from each line
 * - Trailing blank lines are removed
 * - Multiple consecutive spaces within a line are collapsed to one
 * - Line endings are normalized to \n
 *
 * These rules prevent Wrong Answer verdicts caused by trailing newlines,
 * extra spaces, or \r\n vs \n differences — formatting bugs, not logic bugs.
 *
 * The validator NEVER sees raw user output directly — the Execution Agent
 * (or Docker container) captures stdout and passes it as a structured field.
 * This is a security boundary: adversarial output (escape sequences, binary
 * payloads, overlong strings) cannot influence the validator process.
 */
@Slf4j
@Component
public class TestCaseValidator {

    public enum ComparisonMode {
        EXACT,
        TOKEN,
        FLOAT_TOLERANCE
    }

    private static final double DEFAULT_FLOAT_EPSILON = 1e-6;

    /**
     * Validates a single test case: compares actual output against expected.
     *
     * @param actualOutput    stdout captured from the user's program
     * @param expectedOutput  expected output from the test case definition
     * @param mode            comparison mode
     * @return validation result with verdict and details
     */
    public ValidationResult validate(String actualOutput, String expectedOutput,
                                      ComparisonMode mode) {
        if (actualOutput == null) actualOutput = "";
        if (expectedOutput == null) expectedOutput = "";

        String normalizedActual = normalizeWhitespace(actualOutput);
        String normalizedExpected = normalizeWhitespace(expectedOutput);

        return switch (mode) {
            case EXACT -> validateExact(normalizedActual, normalizedExpected);
            case TOKEN -> validateToken(normalizedActual, normalizedExpected);
            case FLOAT_TOLERANCE -> validateFloat(normalizedActual, normalizedExpected,
                    DEFAULT_FLOAT_EPSILON);
        };
    }

    /**
     * Validates a batch of test cases. Returns the first failure, or ACCEPTED if all pass.
     * This implements the pretest fast-fail: on the first Wrong Answer, stop and return.
     */
    public ValidationResult validateBatch(List<TestCase> testCases, List<String> outputs,
                                           ComparisonMode mode) {
        if (testCases.size() != outputs.size()) {
            return new ValidationResult(false, "INTERNAL_ERROR",
                    "Test case count (" + testCases.size() + ") != output count (" + outputs.size() + ")");
        }

        for (int i = 0; i < testCases.size(); i++) {
            ValidationResult result = validate(outputs.get(i), testCases.get(i).expectedOutput(), mode);
            if (!result.accepted()) {
                return new ValidationResult(false, result.verdict(),
                        "Failed on test case " + (i + 1) + ": " + result.details());
            }
        }

        return new ValidationResult(true, "ACCEPTED",
                "All " + testCases.size() + " test cases passed");
    }

    // --- Comparison implementations ---

    private ValidationResult validateExact(String actual, String expected) {
        if (actual.equals(expected)) {
            return new ValidationResult(true, "ACCEPTED", "Exact match");
        }

        // Find the first difference for diagnostics
        String[] actualLines = actual.split("\n", -1);
        String[] expectedLines = expected.split("\n", -1);

        for (int i = 0; i < Math.min(actualLines.length, expectedLines.length); i++) {
            if (!actualLines[i].equals(expectedLines[i])) {
                return new ValidationResult(false, "WRONG_ANSWER",
                        String.format("Line %d: expected '%s', got '%s'",
                                i + 1,
                                truncate(expectedLines[i], 50),
                                truncate(actualLines[i], 50)));
            }
        }

        if (actualLines.length != expectedLines.length) {
            return new ValidationResult(false, "WRONG_ANSWER",
                    String.format("Expected %d lines, got %d lines",
                            expectedLines.length, actualLines.length));
        }

        return new ValidationResult(false, "WRONG_ANSWER", "Output differs");
    }

    /**
     * Token comparison: split both outputs into whitespace-delimited tokens,
     * compare token sequences. More lenient than exact — ignores formatting.
     */
    private ValidationResult validateToken(String actual, String expected) {
        String[] actualTokens = actual.split("\\s+");
        String[] expectedTokens = expected.split("\\s+");

        // Filter out empty tokens (from leading/trailing whitespace)
        actualTokens = filterEmpty(actualTokens);
        expectedTokens = filterEmpty(expectedTokens);

        if (actualTokens.length != expectedTokens.length) {
            return new ValidationResult(false, "WRONG_ANSWER",
                    String.format("Expected %d tokens, got %d tokens",
                            expectedTokens.length, actualTokens.length));
        }

        for (int i = 0; i < actualTokens.length; i++) {
            if (!actualTokens[i].equals(expectedTokens[i])) {
                return new ValidationResult(false, "WRONG_ANSWER",
                        String.format("Token %d: expected '%s', got '%s'",
                                i + 1,
                                truncate(expectedTokens[i], 50),
                                truncate(actualTokens[i], 50)));
            }
        }

        return new ValidationResult(true, "ACCEPTED", "Token match");
    }

    /**
     * Float-tolerant comparison: each token is compared numerically.
     * If both tokens parse as doubles, they are equal if |a - b| <= epsilon
     * or |a - b| / max(1, |b|) <= epsilon (relative tolerance).
     * Non-numeric tokens are compared as strings.
     */
    private ValidationResult validateFloat(String actual, String expected, double epsilon) {
        String[] actualTokens = filterEmpty(actual.split("\\s+"));
        String[] expectedTokens = filterEmpty(expected.split("\\s+"));

        if (actualTokens.length != expectedTokens.length) {
            return new ValidationResult(false, "WRONG_ANSWER",
                    String.format("Expected %d tokens, got %d tokens",
                            expectedTokens.length, actualTokens.length));
        }

        for (int i = 0; i < actualTokens.length; i++) {
            if (!tokensMatch(actualTokens[i], expectedTokens[i], epsilon)) {
                return new ValidationResult(false, "WRONG_ANSWER",
                        String.format("Token %d: expected '%s', got '%s' (epsilon=%e)",
                                i + 1, expectedTokens[i], actualTokens[i], epsilon));
            }
        }

        return new ValidationResult(true, "ACCEPTED", "Float-tolerant match");
    }

    /**
     * Compares two tokens with float tolerance.
     * If both are numeric, uses absolute + relative epsilon comparison.
     * If either is non-numeric, uses exact string comparison.
     */
    private boolean tokensMatch(String actual, String expected, double epsilon) {
        try {
            double a = Double.parseDouble(actual);
            double e = Double.parseDouble(expected);

            // Absolute tolerance
            if (Math.abs(a - e) <= epsilon) return true;

            // Relative tolerance
            double denom = Math.max(1.0, Math.abs(e));
            return Math.abs(a - e) / denom <= epsilon;

        } catch (NumberFormatException ex) {
            // Not numeric — exact string comparison
            return actual.equals(expected);
        }
    }

    // --- Whitespace normalization ---

    /**
     * Normalizes whitespace:
     * 1. Normalize line endings to \n
     * 2. Strip trailing whitespace from each line
     * 3. Collapse multiple spaces to single space within lines
     * 4. Remove trailing blank lines
     */
    String normalizeWhitespace(String input) {
        if (input == null || input.isEmpty()) return "";

        String[] lines = input.replace("\r\n", "\n").replace("\r", "\n").split("\n", -1);
        StringBuilder sb = new StringBuilder();

        int lastNonBlank = -1;
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].strip().replaceAll("\\s+", " ");
            if (!line.isEmpty()) lastNonBlank = i;
        }

        for (int i = 0; i <= lastNonBlank; i++) {
            String line = lines[i].strip().replaceAll("\\s+", " ");
            if (i > 0) sb.append("\n");
            sb.append(line);
        }

        return sb.toString();
    }

    private String[] filterEmpty(String[] tokens) {
        return java.util.Arrays.stream(tokens)
                .filter(t -> !t.isEmpty())
                .toArray(String[]::new);
    }

    private String truncate(String s, int maxLen) {
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }

    // --- Records ---

    public record ValidationResult(boolean accepted, String verdict, String details) {}

    public record TestCase(String input, String expectedOutput) {}
}
