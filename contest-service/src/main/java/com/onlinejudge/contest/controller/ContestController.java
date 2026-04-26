package com.onlinejudge.contest.controller;

import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.service.ContestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/contests")
@RequiredArgsConstructor
public class ContestController {

    private final ContestService contestService;

    @PostMapping
    public ResponseEntity<Contest> create(@RequestBody Map<String, String> body) {
        Contest contest = contestService.createContest(
                body.get("title"),
                Instant.parse(body.get("startTime")),
                Instant.parse(body.get("endTime")));
        return ResponseEntity.status(HttpStatus.CREATED).body(contest);
    }

    @PostMapping("/{contestId}/register")
    public ResponseEntity<Contest> openRegistration(@PathVariable UUID contestId) {
        return ResponseEntity.ok(contestService.openRegistration(contestId));
    }

    @PostMapping("/{contestId}/activate")
    public ResponseEntity<Contest> activate(@PathVariable UUID contestId) {
        return ResponseEntity.ok(contestService.activate(contestId));
    }

    @PostMapping("/{contestId}/close")
    public ResponseEntity<Contest> close(@PathVariable UUID contestId) {
        return ResponseEntity.ok(contestService.close(contestId));
    }

    @PostMapping("/{contestId}/results")
    public ResponseEntity<Contest> publishResults(@PathVariable UUID contestId) {
        return ResponseEntity.ok(contestService.publishResults(contestId));
    }

    @GetMapping("/{contestId}")
    public ResponseEntity<?> getContest(@PathVariable UUID contestId) {
        return contestService.getContest(contestId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * The key delivery endpoint — this is what clients poll starting at T-10s.
     * Returns 404 until the contest is ACTIVE. Once ACTIVE, returns the key.
     * CDN caches this with a short TTL; origin sees minimal traffic.
     */
    @GetMapping("/{contestId}/key")
    public ResponseEntity<?> getDecryptionKey(@PathVariable UUID contestId) {
        return contestService.getDecryptionKey(contestId)
                .map(key -> ResponseEntity.ok(Map.of("key", key)))
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Contest window check — called by API Gateway for submission validation.
     * Returns {"accepting": true/false}.
     */
    @GetMapping("/{contestId}/accepting")
    public ResponseEntity<Map<String, Boolean>> isAccepting(@PathVariable UUID contestId) {
        boolean accepting = contestService.isAcceptingSubmissions(contestId);
        return ResponseEntity.ok(Map.of("accepting", accepting));
    }
}
