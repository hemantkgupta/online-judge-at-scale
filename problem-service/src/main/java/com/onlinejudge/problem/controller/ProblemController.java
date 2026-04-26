package com.onlinejudge.problem.controller;

import com.onlinejudge.problem.model.Problem;
import com.onlinejudge.problem.service.ProblemService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/problems")
@RequiredArgsConstructor
public class ProblemController {

    private final ProblemService problemService;

    @GetMapping
    public ResponseEntity<List<Problem>> listProblems() {
        return ResponseEntity.ok(problemService.getAllProblems());
    }

    @GetMapping("/{problemId}")
    public ResponseEntity<?> getProblem(@PathVariable UUID problemId) {
        return problemService.getProblem(problemId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Returns pre-signed URLs for test case data.
     * Called by the Execution Service to fetch test inputs/outputs.
     *
     * @param pretestOnly if true, return only pretests (ordinal <= 10)
     */
    @GetMapping("/{problemId}/test-cases")
    public ResponseEntity<?> getTestCaseUrls(
            @PathVariable UUID problemId,
            @RequestParam(defaultValue = "true") boolean pretestOnly) {
        var urls = problemService.getTestCaseUrls(problemId, pretestOnly);
        return ResponseEntity.ok(Map.of(
                "problemId", problemId,
                "pretestOnly", pretestOnly,
                "testCases", urls
        ));
    }
}
