package com.onlinejudge.problem.service;

import com.onlinejudge.problem.model.Problem;
import com.onlinejudge.problem.model.TestCase;
import com.onlinejudge.problem.repository.ProblemRepository;
import com.onlinejudge.problem.repository.TestCaseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for the Problem Service.
 *
 * Verifies:
 * - Problem creation with test cases
 * - Pre-signed URL generation for test case delivery
 * - Pretest vs system test filtering (ordinal <= 10)
 * - Test case ordering by ordinal
 */
@ExtendWith(MockitoExtension.class)
class ProblemServiceTest {

    @Mock private ProblemRepository problemRepository;
    @Mock private TestCaseRepository testCaseRepository;
    @InjectMocks private ProblemService problemService;

    @Test
    void createProblem_persistsProblemAndTestCases() {
        when(problemRepository.save(any(Problem.class))).thenAnswer(i -> i.getArgument(0));
        when(testCaseRepository.save(any(TestCase.class))).thenAnswer(i -> i.getArgument(0));

        var testCases = List.of(
                new ProblemService.TestCaseInput("5\n1 2 3 4 5", "15", 10),
                new ProblemService.TestCaseInput("3\n10 20 30", "60", 10),
                new ProblemService.TestCaseInput("1\n1000000", "1000000", 10)
        );

        Problem created = problemService.createProblem("Sum of N", 2000, 256, 100, testCases);

        assertThat(created.getId()).isNotNull();
        assertThat(created.getTitle()).isEqualTo("Sum of N");
        assertThat(created.getTimeLimitMs()).isEqualTo(2000);
        assertThat(created.getMemoryLimitMb()).isEqualTo(256);
        assertThat(created.getPoints()).isEqualTo(100);
        assertThat(created.getTestCaseCount()).isEqualTo(3);
        assertThat(created.getStatementR2Key()).contains("problems/");

        verify(problemRepository, times(1)).save(any(Problem.class));
        verify(testCaseRepository, times(3)).save(any(TestCase.class));
    }

    @Test
    void getTestCaseUrls_pretestOnly_filtersCorrectly() {
        UUID problemId = UUID.randomUUID();
        Problem problem = new Problem();
        problem.setId(problemId);
        when(problemRepository.findById(problemId)).thenReturn(Optional.of(problem));

        // Return 5 pretests (ordinal 1-5)
        List<TestCase> pretests = new java.util.ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            TestCase tc = new TestCase();
            tc.setId(UUID.randomUUID());
            tc.setProblemId(problemId);
            tc.setOrdinal(i);
            tc.setInputR2Key("problems/" + problemId + "/tests/" + i + "/input.txt");
            tc.setExpectedOutputR2Key("problems/" + problemId + "/tests/" + i + "/expected.txt");
            tc.setMaxScore(10);
            pretests.add(tc);
        }
        when(testCaseRepository.findByProblemIdAndOrdinalLessThanEqualOrderByOrdinal(problemId, 10))
                .thenReturn(pretests);

        var urls = problemService.getTestCaseUrls(problemId, true);

        assertThat(urls).hasSize(5);
        assertThat(urls.get(0).ordinal()).isEqualTo(1);
        assertThat(urls.get(0).isPretest()).isTrue();
        assertThat(urls.get(0).inputUrl()).contains("local://");
        assertThat(urls.get(0).inputUrl()).contains("expires=");
    }

    @Test
    void getTestCaseUrls_allTests_includesSystemTests() {
        UUID problemId = UUID.randomUUID();
        Problem problem = new Problem();
        problem.setId(problemId);
        when(problemRepository.findById(problemId)).thenReturn(Optional.of(problem));

        // 5 pretests + 15 system tests = 20 total
        List<TestCase> allTests = new java.util.ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            TestCase tc = new TestCase();
            tc.setId(UUID.randomUUID());
            tc.setProblemId(problemId);
            tc.setOrdinal(i);
            tc.setInputR2Key("test-input-" + i);
            tc.setExpectedOutputR2Key("test-expected-" + i);
            tc.setMaxScore(5);
            allTests.add(tc);
        }
        when(testCaseRepository.findByProblemIdOrderByOrdinal(problemId)).thenReturn(allTests);

        var urls = problemService.getTestCaseUrls(problemId, false);

        assertThat(urls).hasSize(20);
        // First 10 are pretests
        for (int i = 0; i < 10; i++) {
            assertThat(urls.get(i).isPretest()).isTrue();
        }
        // 11-20 are system tests
        for (int i = 10; i < 20; i++) {
            assertThat(urls.get(i).isPretest()).isFalse();
        }
    }

    @Test
    void getTestCaseUrls_nonExistentProblem_throws() {
        UUID fakeId = UUID.randomUUID();
        when(problemRepository.findById(fakeId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> problemService.getTestCaseUrls(fakeId, true))
                .isInstanceOf(java.util.NoSuchElementException.class)
                .hasMessageContaining("Problem not found");
    }

    @Test
    void testCase_pretestDetection() {
        TestCase pretest = new TestCase();
        pretest.setOrdinal(5);
        assertThat(pretest.isPretest()).isTrue();

        TestCase systemTest = new TestCase();
        systemTest.setOrdinal(11);
        assertThat(systemTest.isPretest()).isFalse();

        TestCase boundary = new TestCase();
        boundary.setOrdinal(10);
        assertThat(boundary.isPretest()).isTrue();
    }

    @Test
    void presignedUrls_haveExpiryTimestamp() {
        UUID problemId = UUID.randomUUID();
        Problem problem = new Problem();
        problem.setId(problemId);
        when(problemRepository.findById(problemId)).thenReturn(Optional.of(problem));

        TestCase tc = new TestCase();
        tc.setId(UUID.randomUUID());
        tc.setProblemId(problemId);
        tc.setOrdinal(1);
        tc.setInputR2Key("input.txt");
        tc.setExpectedOutputR2Key("expected.txt");
        tc.setMaxScore(10);
        when(testCaseRepository.findByProblemIdAndOrdinalLessThanEqualOrderByOrdinal(problemId, 10))
                .thenReturn(List.of(tc));

        var urls = problemService.getTestCaseUrls(problemId, true);

        assertThat(urls.get(0).inputUrl()).contains("expires=");
        assertThat(urls.get(0).expectedOutputUrl()).contains("expires=");
    }
}
