package com.onlinejudge.problem.service;

import com.onlinejudge.problem.model.Problem;
import com.onlinejudge.problem.model.TestCase;
import com.onlinejudge.problem.repository.ProblemRepository;
import com.onlinejudge.problem.repository.TestCaseRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

/**
 * Problem lifecycle management.
 *
 * The Problem Service is read-heavy, write-rare:
 * - Reads: every execution worker fetches test cases for every submission
 * - Writes: admin creates problems during contest setup (rare)
 *
 * This asymmetry drives two design decisions:
 * 1. CockroachDB GLOBAL locality: sub-10ms reads from every region
 * 2. Test case delivery via pre-signed R2 URLs: workers fetch directly
 *    from object storage, not through this service
 *
 * Pre-signed URL flow:
 *   1. Execution worker receives a submission event from Kafka
 *   2. Event contains the problem_id
 *   3. Worker calls ProblemService.getTestCaseUrls(problemId)
 *   4. Service returns pre-signed R2 URLs (valid for 5 minutes)
 *   5. Worker fetches test case data directly from R2
 *
 * This keeps the Problem Service out of the hot path — it generates
 * URLs, not data. The actual data transfer is between the worker and R2.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProblemService {

    private final ProblemRepository problemRepository;
    private final TestCaseRepository testCaseRepository;

    /**
     * Creates a new problem with its test cases.
     */
    @Transactional
    public Problem createProblem(String title, int timeLimitMs, int memoryLimitMb,
                                  int points, List<TestCaseInput> testCases) {
        Problem problem = new Problem();
        problem.setId(UUID.randomUUID());
        problem.setTitle(title);
        problem.setTimeLimitMs(timeLimitMs);
        problem.setMemoryLimitMb(memoryLimitMb);
        problem.setPoints(points);
        problem.setTestCaseCount(testCases.size());

        // In production: upload problem statement to R2, get the key
        problem.setStatementR2Key("problems/" + problem.getId() + "/statement.encrypted");
        problemRepository.save(problem);

        // Create test cases
        for (int i = 0; i < testCases.size(); i++) {
            TestCaseInput tc = testCases.get(i);
            TestCase testCase = new TestCase();
            testCase.setId(UUID.randomUUID());
            testCase.setProblemId(problem.getId());
            testCase.setOrdinal(i + 1);
            testCase.setMaxScore(tc.maxScore());

            // In production: upload input/output to R2
            String prefix = "problems/" + problem.getId() + "/tests/" + (i + 1);
            testCase.setInputR2Key(prefix + "/input.txt");
            testCase.setExpectedOutputR2Key(prefix + "/expected.txt");
            testCaseRepository.save(testCase);
        }

        log.info("[problem] Created problem={} title='{}' tests={} points={}",
                problem.getId(), title, testCases.size(), points);

        return problem;
    }

    /**
     * Returns pre-signed URLs for a problem's test cases.
     *
     * The Execution Service calls this to get download URLs for test data.
     * URLs are pre-signed with a 5-minute TTL — the worker must fetch
     * within that window.
     *
     * @param problemId the problem to get test cases for
     * @param pretestOnly if true, return only pretests (ordinal <= 10)
     * @return list of test case URL entries
     */
    public List<TestCaseUrls> getTestCaseUrls(UUID problemId, boolean pretestOnly) {
        Problem problem = problemRepository.findById(problemId)
                .orElseThrow(() -> new NoSuchElementException("Problem not found: " + problemId));

        List<TestCase> testCases = pretestOnly
                ? testCaseRepository.findByProblemIdAndOrdinalLessThanEqualOrderByOrdinal(problemId, 10)
                : testCaseRepository.findByProblemIdOrderByOrdinal(problemId);

        List<TestCaseUrls> urls = new ArrayList<>();
        for (TestCase tc : testCases) {
            // In production: generate pre-signed R2/S3 URLs with 5-minute expiry
            // Locally: return the R2 keys directly (no signing needed)
            urls.add(new TestCaseUrls(
                    tc.getOrdinal(),
                    generatePresignedUrl(tc.getInputR2Key()),
                    generatePresignedUrl(tc.getExpectedOutputR2Key()),
                    tc.getMaxScore(),
                    tc.isPretest()
            ));
        }

        log.debug("[problem] Generated {} test case URLs for problem={} pretestOnly={}",
                urls.size(), problemId, pretestOnly);

        return urls;
    }

    /**
     * Returns problem metadata (without test case data).
     */
    public Optional<Problem> getProblem(UUID problemId) {
        return problemRepository.findById(problemId);
    }

    /**
     * Returns all problems for display (admin panel, problem listing).
     */
    public List<Problem> getAllProblems() {
        return problemRepository.findAll();
    }

    /**
     * Generates a pre-signed URL for an R2/S3 object.
     *
     * In production: uses the R2/S3 SDK to generate a URL signed with
     * the service's credentials, valid for 5 minutes. The Execution
     * Service can download the object without authentication — the
     * signature IS the authentication.
     *
     * Pre-signed URLs are the standard mechanism for granting temporary
     * read access to private objects without exposing credentials.
     *
     * Locally: returns the key as a local:// URL.
     */
    private String generatePresignedUrl(String r2Key) {
        // Production: s3Client.presign(GetObjectRequest.builder()
        //     .bucket("onlinejudge-testcases")
        //     .key(r2Key)
        //     .build(), Duration.ofMinutes(5));
        return "local://" + r2Key + "?expires=" + Instant.now().plusSeconds(300).getEpochSecond();
    }

    // --- DTOs ---

    public record TestCaseInput(String input, String expectedOutput, int maxScore) {}

    public record TestCaseUrls(
            int ordinal,
            String inputUrl,
            String expectedOutputUrl,
            int maxScore,
            boolean isPretest
    ) {}
}
