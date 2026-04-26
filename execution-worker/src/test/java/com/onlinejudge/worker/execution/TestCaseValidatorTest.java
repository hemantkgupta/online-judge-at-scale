package com.onlinejudge.worker.execution;

import com.onlinejudge.worker.execution.TestCaseValidator.ComparisonMode;
import com.onlinejudge.worker.execution.TestCaseValidator.TestCase;
import com.onlinejudge.worker.execution.TestCaseValidator.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the test case output validator.
 *
 * Covers: exact match, token comparison, float tolerance,
 * whitespace normalization, batch validation, edge cases.
 */
class TestCaseValidatorTest {

    private TestCaseValidator validator;

    @BeforeEach
    void setUp() {
        validator = new TestCaseValidator();
    }

    // --- Exact mode ---

    @Test
    void exact_identicalOutput_accepted() {
        var result = validator.validate("42", "42", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
        assertThat(result.verdict()).isEqualTo("ACCEPTED");
    }

    @Test
    void exact_differentOutput_wrongAnswer() {
        var result = validator.validate("43", "42", ComparisonMode.EXACT);
        assertThat(result.accepted()).isFalse();
        assertThat(result.verdict()).isEqualTo("WRONG_ANSWER");
    }

    @Test
    void exact_multilineOutput_accepted() {
        var result = validator.validate("1\n2\n3", "1\n2\n3", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void exact_trailingNewline_normalizedAndAccepted() {
        // Trailing blank lines should be stripped
        var result = validator.validate("42\n", "42", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void exact_trailingWhitespace_normalizedAndAccepted() {
        var result = validator.validate("42  ", "42", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void exact_windowsLineEndings_normalized() {
        var result = validator.validate("1\r\n2\r\n3", "1\n2\n3", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void exact_extraSpacesCollapsed() {
        var result = validator.validate("hello   world", "hello world", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void exact_lineDifference_detailsShowFirstDiff() {
        var result = validator.validate("1\n2\n4", "1\n2\n3", ComparisonMode.EXACT);
        assertThat(result.accepted()).isFalse();
        assertThat(result.details()).contains("Line 3");
    }

    // --- Token mode ---

    @Test
    void token_sameTokensDifferentFormatting_accepted() {
        var result = validator.validate("1  2  3", "1 2 3", ComparisonMode.TOKEN);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void token_newlinesVsSpaces_accepted() {
        var result = validator.validate("1\n2\n3", "1 2 3", ComparisonMode.TOKEN);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void token_wrongTokenCount_rejected() {
        var result = validator.validate("1 2", "1 2 3", ComparisonMode.TOKEN);
        assertThat(result.accepted()).isFalse();
        assertThat(result.details()).contains("3 tokens");
    }

    @Test
    void token_wrongValue_detailsShowWhichToken() {
        var result = validator.validate("1 99 3", "1 2 3", ComparisonMode.TOKEN);
        assertThat(result.accepted()).isFalse();
        assertThat(result.details()).contains("Token 2");
    }

    // --- Float tolerance mode ---

    @Test
    void float_withinEpsilon_accepted() {
        var result = validator.validate("3.14159265", "3.14159266", ComparisonMode.FLOAT_TOLERANCE);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void float_outsideEpsilon_rejected() {
        var result = validator.validate("3.14", "3.15", ComparisonMode.FLOAT_TOLERANCE);
        assertThat(result.accepted()).isFalse();
    }

    @Test
    void float_mixedNumericAndString_stringExactMatch() {
        var result = validator.validate("YES 3.14159265", "YES 3.14159266", ComparisonMode.FLOAT_TOLERANCE);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void float_stringMismatch_rejected() {
        var result = validator.validate("NO 3.14", "YES 3.14", ComparisonMode.FLOAT_TOLERANCE);
        assertThat(result.accepted()).isFalse();
    }

    @Test
    void float_relativeToleranceForLargeNumbers() {
        // 1000000.0 vs 1000000.0000001 — relative tolerance
        var result = validator.validate("1000000.0", "1000000.0000001", ComparisonMode.FLOAT_TOLERANCE);
        assertThat(result.accepted()).isTrue();
    }

    // --- Batch validation ---

    @Test
    void batch_allPass_accepted() {
        var testCases = List.of(
                new TestCase("", "42"),
                new TestCase("", "100"),
                new TestCase("", "hello")
        );
        var outputs = List.of("42", "100", "hello");

        var result = validator.validateBatch(testCases, outputs, ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
        assertThat(result.details()).contains("All 3 test cases passed");
    }

    @Test
    void batch_secondTestFails_reportsWhichOne() {
        var testCases = List.of(
                new TestCase("", "42"),
                new TestCase("", "100"),
                new TestCase("", "hello")
        );
        var outputs = List.of("42", "99", "hello");

        var result = validator.validateBatch(testCases, outputs, ComparisonMode.EXACT);
        assertThat(result.accepted()).isFalse();
        assertThat(result.details()).contains("test case 2");
    }

    @Test
    void batch_mismatchedCounts_internalError() {
        var testCases = List.of(new TestCase("", "42"));
        var outputs = List.of("42", "100");

        var result = validator.validateBatch(testCases, outputs, ComparisonMode.EXACT);
        assertThat(result.accepted()).isFalse();
        assertThat(result.verdict()).isEqualTo("INTERNAL_ERROR");
    }

    // --- Edge cases ---

    @Test
    void nullOutput_treatedAsEmpty() {
        var result = validator.validate(null, "", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
    }

    @Test
    void emptyOutput_matchesEmptyExpected() {
        var result = validator.validate("", "", ComparisonMode.EXACT);
        assertThat(result.accepted()).isTrue();
    }

    // --- Whitespace normalization ---

    @Test
    void normalizeWhitespace_stripsTrailingBlankLines() {
        assertThat(validator.normalizeWhitespace("hello\n\n\n")).isEqualTo("hello");
    }

    @Test
    void normalizeWhitespace_preservesInternalBlankLines() {
        assertThat(validator.normalizeWhitespace("a\n\nb")).isEqualTo("a\n\nb");
    }

    @Test
    void normalizeWhitespace_collapsesSpaces() {
        assertThat(validator.normalizeWhitespace("a  b   c")).isEqualTo("a b c");
    }
}
