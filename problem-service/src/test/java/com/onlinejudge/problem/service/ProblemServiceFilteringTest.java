package com.onlinejudge.problem.service;

import com.onlinejudge.problem.entity.Problem;
import com.onlinejudge.problem.entity.TestCase;
import com.onlinejudge.problem.repository.ProblemRepository;
import com.onlinejudge.problem.repository.TestCaseRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

/**
 * Verifies the contract from the spec: when 12 test cases exist,
 * {@code pretestOnly=true} returns 10 and {@code pretestOnly=false} returns 12.
 */
@ExtendWith(MockitoExtension.class)
class ProblemServiceFilteringTest {

    @Mock private ProblemRepository problemRepository;
    @Mock private TestCaseRepository testCaseRepository;
    @Mock private GcsSigner gcsSigner;
    @InjectMocks private ProblemService problemService;

    @Test
    void getTestCaseUrls_pretestVsFull_filtersTo10vs12() {
        UUID problemId = UUID.randomUUID();
        Problem problem = new Problem();
        problem.setId(problemId);
        problem.setTitle("filtering-test");
        problem.setTimeLimitMs(1000);
        problem.setMemoryLimitMb(256);
        problem.setPoints(100);
        when(problemRepository.findById(problemId)).thenReturn(Optional.of(problem));
        when(gcsSigner.sign(anyString())).thenAnswer(inv -> "https://signed/" + inv.getArgument(0));

        // 12 test cases total: ordinals 1..12. First 10 are pretests.
        List<TestCase> all = buildTestCases(problemId, 12);
        when(testCaseRepository.findByProblemIdOrderByOrdinal(problemId)).thenReturn(all);
        when(testCaseRepository.findByProblemIdAndOrdinalLessThanEqualOrderByOrdinal(eq(problemId), eq(10)))
                .thenReturn(all.subList(0, 10));

        var pretests = problemService.getTestCaseUrls(problemId, true);
        var full = problemService.getTestCaseUrls(problemId, false);

        assertThat(pretests).hasSize(10);
        assertThat(pretests.get(0).ordinal()).isEqualTo(1);
        assertThat(pretests.get(9).ordinal()).isEqualTo(10);
        assertThat(pretests.get(0).inputUrl()).startsWith("https://signed/");
        assertThat(pretests.get(0).expectedOutputUrl()).startsWith("https://signed/");

        assertThat(full).hasSize(12);
        assertThat(full.get(11).ordinal()).isEqualTo(12);
    }

    /**
     * Tech-spec §14 L3: the pretest/system-test threshold is now a config
     * knob. With {@code app.problem.pretest-ordinal-threshold=5}, only the
     * first 5 ordinals are returned for {@code pretestOnly=true}.
     */
    @Test
    void getTestCaseUrls_respectsConfigurablePretestThreshold() {
        UUID problemId = UUID.randomUUID();
        Problem problem = new Problem();
        problem.setId(problemId);
        problem.setTitle("threshold-override");
        problem.setTimeLimitMs(1000);
        problem.setMemoryLimitMb(256);
        problem.setPoints(100);
        when(problemRepository.findById(problemId)).thenReturn(Optional.of(problem));
        when(gcsSigner.sign(anyString())).thenAnswer(inv -> "https://signed/" + inv.getArgument(0));

        // Override the threshold to 5 (vs the default 10) — Spring would
        // normally set this via @Value at startup; ReflectionTestUtils
        // mirrors that for the mock-wired unit test.
        ReflectionTestUtils.setField(problemService, "pretestOrdinalThreshold", 5);

        List<TestCase> all = buildTestCases(problemId, 12);
        when(testCaseRepository.findByProblemIdAndOrdinalLessThanEqualOrderByOrdinal(eq(problemId), eq(5)))
                .thenReturn(all.subList(0, 5));

        var pretests = problemService.getTestCaseUrls(problemId, true);

        assertThat(pretests).hasSize(5);
        assertThat(pretests.get(0).ordinal()).isEqualTo(1);
        assertThat(pretests.get(4).ordinal()).isEqualTo(5);
    }

    /**
     * Default behaviour (unset property) preserves the pre-§14-L3 contract:
     * threshold = 10. Belt-and-braces test alongside the existing
     * {@code 10 vs 12} check.
     */
    @Test
    void getTestCaseUrls_defaultThresholdMatchesLegacyBlogContract() {
        assertThat(ReflectionTestUtils.getField(problemService, "pretestOrdinalThreshold"))
                .isEqualTo(ProblemService.DEFAULT_PRETEST_ORDINAL_THRESHOLD)
                .isEqualTo(10);
    }

    private List<TestCase> buildTestCases(UUID problemId, int n) {
        List<TestCase> list = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            TestCase tc = new TestCase();
            tc.setId(UUID.randomUUID());
            tc.setProblemId(problemId);
            tc.setOrdinal(i);
            tc.setInputGcsKey("problems/" + problemId + "/tests/" + i + "/input.txt");
            tc.setExpectedOutputGcsKey("problems/" + problemId + "/tests/" + i + "/expected.txt");
            list.add(tc);
        }
        return list;
    }
}
