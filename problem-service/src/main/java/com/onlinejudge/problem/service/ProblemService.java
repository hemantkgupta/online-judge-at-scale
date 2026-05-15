package com.onlinejudge.problem.service;

import com.onlinejudge.problem.dto.CreateProblemRequest;
import com.onlinejudge.problem.dto.CreateTestCaseRequest;
import com.onlinejudge.problem.dto.TestCaseUrlsDto;
import com.onlinejudge.problem.entity.Problem;
import com.onlinejudge.problem.entity.TestCase;
import com.onlinejudge.problem.repository.ProblemRepository;
import com.onlinejudge.problem.repository.TestCaseRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

/**
 * Problem lifecycle + test-case URL generation.
 *
 * <p>The {@code /test-cases} endpoint is the hot read path: every execution
 * worker hits it once per submission. Cost per call is:
 * <ol>
 *   <li>One indexed scan on {@code test_cases (problem_id, ordinal)} — 5–50 rows.</li>
 *   <li>One {@link GcsSigner#sign} call per row, per URL (2 URLs/row, both V4 HMAC).</li>
 * </ol>
 * No GCS RPCs — signing is local crypto. CockroachDB GLOBAL locality keeps
 * the read sub-10ms from every region.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProblemService {

    /** Ordinals 1–10 are pretests. Matches the contract in the blog post. */
    public static final int PRETEST_MAX_ORDINAL = 10;

    private final ProblemRepository problemRepository;
    private final TestCaseRepository testCaseRepository;
    private final GcsSigner gcsSigner;

    public List<Problem> listProblems() {
        return problemRepository.findAll();
    }

    public Optional<Problem> getProblem(UUID id) {
        return problemRepository.findById(id);
    }

    @Transactional
    public Problem createProblem(CreateProblemRequest req) {
        Problem p = new Problem();
        p.setId(UUID.randomUUID());
        p.setTitle(req.title());
        p.setTimeLimitMs(req.timeLimitMs());
        p.setMemoryLimitMb(req.memoryLimitMb());
        p.setPoints(req.points());
        Problem saved = problemRepository.save(p);
        log.info("[problem] created id={} title='{}'", saved.getId(), saved.getTitle());
        return saved;
    }

    @Transactional
    public void registerTestCases(UUID problemId, CreateTestCaseRequest req) {
        Problem problem = problemRepository.findById(problemId)
                .orElseThrow(() -> new NoSuchElementException("Problem not found: " + problemId));
        for (CreateTestCaseRequest.TestCaseEntry e : req.testCases()) {
            TestCase tc = new TestCase();
            tc.setId(UUID.randomUUID());
            tc.setProblemId(problem.getId());
            tc.setOrdinal(e.ordinal());
            tc.setInputGcsKey(e.inputGcsKey());
            tc.setExpectedOutputGcsKey(e.expectedOutputGcsKey());
            testCaseRepository.save(tc);
        }
        log.info("[problem] registered {} test cases for problem={}", req.testCases().size(), problemId);
    }

    /**
     * Returns ordered test-case URLs for the given problem. Each URL is a V4-
     * signed GCS download URL valid for 5 minutes.
     *
     * @throws NoSuchElementException if the problem does not exist
     */
    public List<TestCaseUrlsDto> getTestCaseUrls(UUID problemId, boolean pretestOnly) {
        if (!problemRepository.existsById(problemId)) {
            throw new NoSuchElementException("Problem not found: " + problemId);
        }
        List<TestCase> rows = pretestOnly
                ? testCaseRepository.findByProblemIdAndOrdinalLessThanEqualOrderByOrdinal(problemId, PRETEST_MAX_ORDINAL)
                : testCaseRepository.findByProblemIdOrderByOrdinal(problemId);

        List<TestCaseUrlsDto> out = new ArrayList<>(rows.size());
        for (TestCase tc : rows) {
            out.add(new TestCaseUrlsDto(
                    tc.getOrdinal(),
                    gcsSigner.sign(tc.getInputGcsKey()),
                    gcsSigner.sign(tc.getExpectedOutputGcsKey())
            ));
        }
        log.debug("[problem] signed {} test-case URLs for problem={} pretestOnly={}",
                out.size(), problemId, pretestOnly);
        return out;
    }
}
