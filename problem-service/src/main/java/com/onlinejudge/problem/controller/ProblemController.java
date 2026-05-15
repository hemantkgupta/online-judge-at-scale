package com.onlinejudge.problem.controller;

import com.onlinejudge.problem.dto.CreateProblemRequest;
import com.onlinejudge.problem.dto.CreateTestCaseRequest;
import com.onlinejudge.problem.dto.ProblemDto;
import com.onlinejudge.problem.dto.TestCaseUrlsDto;
import com.onlinejudge.problem.service.ProblemService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * HTTP surface of the Problem Service.
 *
 * <p>No auth wired up yet — admin endpoints rely on network isolation
 * (this service is reachable only from the {@code oj-control-plane} VM /
 * the internal VPC). Workstream G layers JWT + admin role on top.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/problems")
@RequiredArgsConstructor
public class ProblemController {

    private final ProblemService problemService;

    @GetMapping
    public List<ProblemDto> list() {
        return problemService.listProblems().stream().map(ProblemDto::from).toList();
    }

    @GetMapping("/{id}")
    public ResponseEntity<ProblemDto> get(@PathVariable UUID id) {
        return problemService.getProblem(id)
                .map(ProblemDto::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<ProblemDto> create(@Valid @RequestBody CreateProblemRequest req) {
        var saved = problemService.createProblem(req);
        return ResponseEntity.status(HttpStatus.CREATED).body(ProblemDto.from(saved));
    }

    @PostMapping("/{id}/test-cases")
    public ResponseEntity<Void> registerTestCases(@PathVariable UUID id,
                                                  @Valid @RequestBody CreateTestCaseRequest req) {
        problemService.registerTestCases(id, req);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    /**
     * The critical endpoint. Returns an ordered list of test-case URLs.
     *
     * @param pretestOnly if true, only ordinals 1–10 are returned
     */
    @GetMapping("/{id}/test-cases")
    public List<TestCaseUrlsDto> getTestCaseUrls(@PathVariable UUID id,
                                                 @RequestParam(defaultValue = "false") boolean pretestOnly) {
        return problemService.getTestCaseUrls(id, pretestOnly);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<String> notFound(NoSuchElementException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }
}
