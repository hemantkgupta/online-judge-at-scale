package com.onlinejudge.leaderboard.controller;

import com.onlinejudge.leaderboard.service.LeaderboardService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/leaderboard")
@RequiredArgsConstructor
public class LeaderboardController {

    private final LeaderboardService leaderboardService;

    /**
     * GET /api/v1/leaderboard/{contestId}?page=0&size=100
     * Returns a page of the leaderboard. Pure Redis ZREVRANGE - no DB query.
     */
    @GetMapping("/{contestId}")
    public ResponseEntity<Map<String, Object>> getLeaderboard(
            @PathVariable String contestId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int size) {

        List<Map<String, Object>> rows = leaderboardService.getLeaderboardPage(contestId, page, size);
        long total = leaderboardService.getParticipantCount(contestId);

        return ResponseEntity.ok(Map.of(
                "contestId", contestId,
                "page", page,
                "pageSize", size,
                "totalParticipants", total,
                "entries", rows
        ));
    }

    /**
     * GET /api/v1/leaderboard/{contestId}/rank/{userId}
     * Returns a user's current rank and score.
     */
    @GetMapping("/{contestId}/rank/{userId}")
    public ResponseEntity<Map<String, Object>> getUserRank(
            @PathVariable String contestId,
            @PathVariable String userId) {

        Map<String, Object> result = leaderboardService.getUserScore(contestId, userId);
        return ResponseEntity.ok(result);
    }
}
