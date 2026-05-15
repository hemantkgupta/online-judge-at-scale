package com.onlinejudge.problem.service;

import com.onlinejudge.problem.entity.TestCase;
import com.onlinejudge.problem.repository.ProblemRepository;
import com.onlinejudge.problem.repository.TestCaseRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
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
        when(problemRepository.existsById(problemId)).thenReturn(true);
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
