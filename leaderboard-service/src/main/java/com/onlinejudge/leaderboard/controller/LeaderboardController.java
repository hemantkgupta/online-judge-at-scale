package com.onlinejudge.leaderboard.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.leaderboard.dto.LeaderboardEntry;
import com.onlinejudge.leaderboard.dto.LeaderboardResponse;
import com.onlinejudge.leaderboard.service.LeaderboardService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Leaderboard REST surface.
 *
 * <p>This single controller covers both the local-only and the cross-region
 * paths. The {@code global} query flag flips between them:
 *
 * <ul>
 *   <li>{@code global=false} (default) — return THIS region's leaderboard only.
 *       This is also the path the peer region's controller fans out to in order
 *       to avoid recursion.</li>
 *   <li>{@code global=true} — return this region's leaderboard merged with the
 *       peer region's. The peer is queried over HTTP at
 *       {@code ${app.peer-leaderboard.url}/api/v1/leaderboard/{contestId}?global=false&page=0&size=…},
 *       with a 5 s read timeout. On any peer failure (timeout, 5xx, URL
 *       unset) the path degrades to local-only and surfaces both an
 *       {@code X-Peer-Region-Unreachable: true} header and a
 *       {@code degraded: true} JSON flag — degraded &gt; broken.</li>
 * </ul>
 *
 * <p><b>Merge semantics.</b> In sticky-region world a user's submissions are
 * pinned to their home region's broker, scoring-pipeline, and ZSET. So each
 * userId appears in EXACTLY ONE region's leaderboard. The merge is a
 * <em>disjoint union</em>: combine both lists, sort by composite score
 * descending, re-rank from 1, slice to the requested page. If the same
 * userId shows up in both regions that's a data anomaly (e.g. a misrouted
 * submission); we keep the local-region's row and log. We do <b>not</b> sum
 * scores across regions — there is no expected overlap to sum.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/leaderboard")
public class LeaderboardController {

    private static final Duration PEER_CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration PEER_REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final int PEER_FETCH_HARD_CAP = 1000;

    private final LeaderboardService leaderboardService;
    private final ObjectMapper objectMapper;
    /** Indirection so tests can swap in a fake HTTP layer. */
    private final Function<HttpRequest, HttpResponse<String>> peerFetcher;

    @Value("${app.region:asia-south1}")
    private String currentRegion;

    @Value("${app.peer-leaderboard.url:}")
    private String peerUrl;

    @org.springframework.beans.factory.annotation.Autowired
    public LeaderboardController(LeaderboardService leaderboardService, ObjectMapper objectMapper) {
        this(leaderboardService, objectMapper, defaultPeerFetcher());
    }

    /** Test seam: inject a synthetic {@code peerFetcher}. */
    public LeaderboardController(LeaderboardService leaderboardService,
                                 ObjectMapper objectMapper,
                                 Function<HttpRequest, HttpResponse<String>> peerFetcher) {
        this.leaderboardService = leaderboardService;
        this.objectMapper = objectMapper;
        this.peerFetcher = peerFetcher;
    }

    private static Function<HttpRequest, HttpResponse<String>> defaultPeerFetcher() {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(PEER_CONNECT_TIMEOUT)
                .build();
        return req -> {
            try {
                return client.send(req, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new PeerFetchException(ex);
            }
        };
    }

    /**
     * GET /api/v1/leaderboard/{contestId}?global=false&page=0&size=100
     *
     * <p>{@code global=true} merges the peer region's leaderboard in.
     * {@code global=false} (default) returns this region only — also the
     * shape the peer's global-fan-out hits.
     */
    @GetMapping(value = "/{contestId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<LeaderboardResponse> getLeaderboard(
            @PathVariable String contestId,
            @RequestParam(defaultValue = "false") boolean global,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int size) {

        // Local rows: stamped with THIS region so cross-region callers can attribute them.
        List<LeaderboardEntry> local = stampRegion(
                leaderboardService.getTop(contestId, page, size), currentRegion);
        long localTotal = leaderboardService.getParticipantCount(contestId);

        if (!global) {
            return ResponseEntity.ok(new LeaderboardResponse(
                    contestId, page, size, localTotal, local, false));
        }

        // Peer URL not configured → can't fan out. Degrade cleanly.
        if (peerUrl == null || peerUrl.isBlank()) {
            log.debug("[leaderboard] global=true but app.peer-leaderboard.url is unset; serving local-only.");
            return degraded(contestId, page, size, local, localTotal);
        }

        // Pull a wider window from the peer than the page itself so the
        // merge has enough headroom to re-slice. Bounded by PEER_FETCH_HARD_CAP
        // (= existing in-service hard cap of 500 doubled) so a runaway page
        // number doesn't pull the peer's entire contest.
        int peerSize = Math.min((page + 1) * Math.max(size, 1), PEER_FETCH_HARD_CAP);
        URI peerUri = buildPeerUri(contestId, peerSize);

        HttpRequest request = HttpRequest.newBuilder(peerUri)
                .timeout(PEER_REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .GET()
                .build();

        List<LeaderboardEntry> peerEntries;
        long peerTotal;
        try {
            HttpResponse<String> resp = peerFetcher.apply(request);
            if (resp.statusCode() / 100 != 2) {
                log.warn("[leaderboard] peer fan-out non-2xx: status={} uri={}",
                        resp.statusCode(), peerUri);
                return degraded(contestId, page, size, local, localTotal);
            }
            ParsedPeer parsed = parsePeer(resp.body());
            peerEntries = parsed.entries;
            peerTotal = parsed.total;
        } catch (PeerFetchException ex) {
            log.warn("[leaderboard] peer fan-out failed: {}", ex.getMessage());
            return degraded(contestId, page, size, local, localTotal);
        } catch (Exception ex) {
            // Parse failures, etc. — treat as a peer outage so the user
            // still sees a leaderboard.
            log.warn("[leaderboard] peer fan-out parse failure: {}", ex.getMessage());
            return degraded(contestId, page, size, local, localTotal);
        }

        List<LeaderboardEntry> merged = mergeAndRerank(local, peerEntries, page, size);
        return ResponseEntity.ok(new LeaderboardResponse(
                contestId, page, size, localTotal + peerTotal, merged, false));
    }

    /**
     * GET /api/v1/leaderboard/{contestId}/rank/{userId}
     * Local-region rank lookup. No cross-region semantics — the user lives in
     * one home region.
     */
    @GetMapping("/{contestId}/rank/{userId}")
    public ResponseEntity<Map<String, Object>> getUserRank(
            @PathVariable String contestId,
            @PathVariable String userId) {
        return ResponseEntity.ok(leaderboardService.getUserScore(contestId, userId));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static List<LeaderboardEntry> stampRegion(List<LeaderboardEntry> rows, String region) {
        List<LeaderboardEntry> out = new ArrayList<>(rows.size());
        for (LeaderboardEntry row : rows) {
            out.add(row.withRegion(region));
        }
        return out;
    }

    private ResponseEntity<LeaderboardResponse> degraded(String contestId, int page, int size,
                                                          List<LeaderboardEntry> local, long localTotal) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Peer-Region-Unreachable", "true");
        return ResponseEntity.ok().headers(headers).body(
                new LeaderboardResponse(contestId, page, size, localTotal, local, true));
    }

    private URI buildPeerUri(String contestId, int peerSize) {
        String base = peerUrl.endsWith("/") ? peerUrl.substring(0, peerUrl.length() - 1) : peerUrl;
        String encodedContest = URLEncoder.encode(contestId, StandardCharsets.UTF_8);
        return URI.create(base + "/api/v1/leaderboard/" + encodedContest
                + "?global=false&page=0&size=" + peerSize);
    }

    /**
     * Merge local + peer disjoint-union by userId, sort by composite zsetScore
     * desc (fallback: points desc, then penalty asc), re-rank from 1, slice to
     * the requested window. If a userId collides across regions (shouldn't
     * happen in sticky-region world), local wins and the peer row is dropped.
     */
    static List<LeaderboardEntry> mergeAndRerank(List<LeaderboardEntry> local,
                                                  List<LeaderboardEntry> peer,
                                                  int page, int size) {
        Set<String> seen = new HashSet<>();
        List<LeaderboardEntry> merged = new ArrayList<>(local.size() + peer.size());
        for (LeaderboardEntry e : local) {
            if (e.getUserId() != null && seen.add(e.getUserId())) {
                merged.add(e);
            }
        }
        for (LeaderboardEntry e : peer) {
            if (e.getUserId() != null && seen.add(e.getUserId())) {
                merged.add(e);
            } else if (e.getUserId() != null) {
                log.warn("[leaderboard] cross-region userId collision; dropping peer row userId={}",
                        e.getUserId());
            }
        }

        merged.sort(Comparator
                .comparing((LeaderboardEntry e) -> e.getZsetScore() == null ? Double.NEGATIVE_INFINITY : e.getZsetScore())
                .reversed()
                .thenComparingLong(e -> -e.getPoints())
                .thenComparingLong(LeaderboardEntry::getPenaltyMinutes)
                .thenComparing(e -> e.getUserId() == null ? "" : e.getUserId()));

        int from = Math.max(0, page * size);
        int to = Math.min(merged.size(), from + size);
        if (from >= merged.size()) {
            return Collections.emptyList();
        }
        List<LeaderboardEntry> slice = merged.subList(from, to);
        List<LeaderboardEntry> ranked = new ArrayList<>(slice.size());
        long rank = from + 1L;
        for (LeaderboardEntry e : slice) {
            ranked.add(e.withRank(rank++));
        }
        return ranked;
    }

    private ParsedPeer parsePeer(String body) throws IOException {
        JsonNode root = objectMapper.readTree(body);
        JsonNode entriesNode = root.path("entries");
        List<LeaderboardEntry> entries = new ArrayList<>();
        if (entriesNode.isArray()) {
            for (JsonNode row : entriesNode) {
                LeaderboardEntry e = new LeaderboardEntry();
                e.setRank(row.path("rank").asLong(0));
                e.setUserId(row.path("userId").asText(null));
                JsonNode zs = row.get("zsetScore");
                e.setZsetScore(zs == null || zs.isNull() ? null : zs.asDouble());
                e.setPoints(row.path("points").asLong(0));
                e.setPenaltyMinutes(row.path("penaltyMinutes").asLong(0));
                // Peer should already have stamped its own region. If missing,
                // we leave it null rather than guess.
                JsonNode regionNode = row.get("region");
                e.setRegion(regionNode == null || regionNode.isNull() ? null : regionNode.asText());
                entries.add(e);
            }
        }
        long total = root.path("totalParticipants").asLong(entries.size());
        return new ParsedPeer(entries, total);
    }

    private record ParsedPeer(List<LeaderboardEntry> entries, long total) {}

    /** Wraps any peer-fetch IO/interrupted error so the controller can degrade. */
    static final class PeerFetchException extends RuntimeException {
        PeerFetchException(Throwable cause) { super(cause); }
    }
}
