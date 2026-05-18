package com.onlinejudge.leaderboard.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.leaderboard.dto.LeaderboardEntry;
import com.onlinejudge.leaderboard.dto.LeaderboardResponse;
import com.onlinejudge.leaderboard.service.LeaderboardService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.net.ssl.SSLSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the cross-region merge controller path.
 *
 * <p>Merge semantics asserted: <b>disjoint union by userId, sort by zsetScore
 * desc, re-rank from 1, slice to page window</b>. Each userId is expected to
 * live in exactly one region (sticky-region world) so no summing.
 */
class LeaderboardControllerGlobalTest {

    private LeaderboardService service;
    private ObjectMapper mapper;
    private AtomicReference<HttpRequest> capturedRequest;

    @BeforeEach
    void setUp() {
        service = mock(LeaderboardService.class);
        mapper = new ObjectMapper();
        capturedRequest = new AtomicReference<>();
    }

    private LeaderboardController newController(Function<HttpRequest, HttpResponse<String>> fetcher,
                                                 String currentRegion, String peerUrl) {
        LeaderboardController c = new LeaderboardController(service, mapper, fetcher);
        ReflectionTestUtils.setField(c, "currentRegion", currentRegion);
        ReflectionTestUtils.setField(c, "peerUrl", peerUrl);
        return c;
    }

    private Function<HttpRequest, HttpResponse<String>> capturing(Function<HttpRequest, HttpResponse<String>> inner) {
        return req -> {
            capturedRequest.set(req);
            return inner.apply(req);
        };
    }

    @Test
    void globalFalse_returnsLocalOnly_withoutAnyHttpFanout() {
        when(service.getTop("c-1", 0, 10)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0),
                row(2, "user-B",  50.0, 50, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(2L);

        AtomicInteger called = new AtomicInteger();
        LeaderboardController controller = newController(
                req -> { called.incrementAndGet(); return fakeResponse(200, "{}"); },
                "asia-south1", "http://peer:8082");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", false, 0, 10);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody().isDegraded()).isFalse();
        assertThat(resp.getBody().getEntries()).hasSize(2);
        assertThat(resp.getHeaders().getFirst("X-Peer-Region-Unreachable")).isNull();
        // EVERY local entry is stamped with the current region.
        assertThat(resp.getBody().getEntries())
                .allSatisfy(e -> assertThat(e.getRegion()).isEqualTo("asia-south1"));
        // No HTTP call.
        assertThat(called.get()).isZero();
    }

    @Test
    void globalTrue_peerHappy_mergedListIsDisjointUnionRerankedAndSliced() throws IOException {
        // Local region: A=100, B=50. Peer region: C=80, D=60.
        when(service.getTop("c-1", 0, 4)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0),
                row(2, "user-B",  50.0, 50, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(2L);

        String peerBody = mapper.writeValueAsString(Map.of(
                "contestId", "c-1",
                "page", 0,
                "pageSize", 4,
                "totalParticipants", 2L,
                "entries", List.of(
                        Map.of("rank", 1, "userId", "user-C", "zsetScore", 80.0,
                               "points", 80, "penaltyMinutes", 0, "region", "us-central1"),
                        Map.of("rank", 2, "userId", "user-D", "zsetScore", 60.0,
                               "points", 60, "penaltyMinutes", 0, "region", "us-central1")),
                "degraded", false));

        LeaderboardController controller = newController(
                capturing(req -> fakeResponse(200, peerBody)),
                "asia-south1", "http://peer:8082");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", true, 0, 4);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody().isDegraded()).isFalse();
        // Merged + re-ranked desc by zsetScore: A=100, C=80, D=60, B=50.
        List<LeaderboardEntry> merged = resp.getBody().getEntries();
        assertThat(merged).extracting(LeaderboardEntry::getUserId)
                .containsExactly("user-A", "user-C", "user-D", "user-B");
        assertThat(merged).extracting(LeaderboardEntry::getRank)
                .containsExactly(1L, 2L, 3L, 4L);
        // Region stamps: local rows say asia-south1, peer rows say us-central1.
        assertThat(merged.get(0).getRegion()).isEqualTo("asia-south1");
        assertThat(merged.get(1).getRegion()).isEqualTo("us-central1");
        assertThat(merged.get(2).getRegion()).isEqualTo("us-central1");
        assertThat(merged.get(3).getRegion()).isEqualTo("asia-south1");
        // Peer fan-out targeted /api/v1/leaderboard/c-1?global=false (no recursion).
        assertThat(capturedRequest.get().uri().toString())
                .contains("/api/v1/leaderboard/c-1")
                .contains("global=false");
    }

    @Test
    void globalTrue_peer5xx_returnsLocalOnlyWithDegradedHeaderAndFlag() {
        when(service.getTop("c-1", 0, 10)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(1L);

        LeaderboardController controller = newController(
                req -> fakeResponse(503, "service unavailable"),
                "asia-south1", "http://peer:8082");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", true, 0, 10);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody().isDegraded()).isTrue();
        assertThat(resp.getBody().getEntries()).hasSize(1);
        assertThat(resp.getHeaders().getFirst("X-Peer-Region-Unreachable")).isEqualTo("true");
    }

    @Test
    void globalTrue_peerUrlUnset_returnsLocalOnlyDegraded() {
        when(service.getTop("c-1", 0, 10)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(1L);

        AtomicInteger called = new AtomicInteger();
        LeaderboardController controller = newController(
                req -> { called.incrementAndGet(); return fakeResponse(200, "{}"); },
                "asia-south1", "");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", true, 0, 10);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody().isDegraded()).isTrue();
        assertThat(resp.getHeaders().getFirst("X-Peer-Region-Unreachable")).isEqualTo("true");
        // No HTTP attempt when peer URL is unset.
        assertThat(called.get()).isZero();
    }

    @Test
    void globalTrue_peerTimeout_returnsLocalOnlyDegraded() {
        when(service.getTop("c-1", 0, 10)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(1L);

        LeaderboardController controller = newController(
                req -> { throw new LeaderboardController.PeerFetchException(
                        new java.net.http.HttpTimeoutException("timed out after 5s")); },
                "asia-south1", "http://peer:8082");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", true, 0, 10);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody().isDegraded()).isTrue();
        assertThat(resp.getHeaders().getFirst("X-Peer-Region-Unreachable")).isEqualTo("true");
    }

    @Test
    void regionTaggingApplies_toEveryReturnedEntry_evenInLocalOnlyResponses() {
        when(service.getTop("c-1", 0, 3)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0),
                row(2, "user-B",  50.0, 50, 0),
                row(3, "user-C",  10.0, 10, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(3L);

        LeaderboardController controller = newController(
                req -> fakeResponse(200, "{}"), "asia-south1", "");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", false, 0, 3);

        assertThat(resp.getBody().getEntries())
                .extracting(LeaderboardEntry::getRegion)
                .containsOnly("asia-south1");
    }

    @Test
    void merge_disjointUnion_noOverlap_isPreserved() throws IOException {
        // Sticky-region invariant: each userId appears in EXACTLY ONE region.
        // Asserts that with strictly disjoint inputs (no overlapping userIds)
        // the merged size = local + peer and every input row survives.
        when(service.getTop("c-1", 0, 10)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0),
                row(2, "user-B",  50.0, 50, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(2L);

        String peerBody = mapper.writeValueAsString(Map.of(
                "contestId", "c-1",
                "totalParticipants", 2L,
                "entries", List.of(
                        Map.of("rank", 1, "userId", "user-X", "zsetScore", 90.0,
                               "points", 90, "penaltyMinutes", 0, "region", "us-central1"),
                        Map.of("rank", 2, "userId", "user-Y", "zsetScore", 40.0,
                               "points", 40, "penaltyMinutes", 0, "region", "us-central1")),
                "degraded", false));

        LeaderboardController controller = newController(
                req -> fakeResponse(200, peerBody),
                "asia-south1", "http://peer:8082");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", true, 0, 10);

        assertThat(resp.getBody().isDegraded()).isFalse();
        // Disjoint union = 4 rows, sorted desc by zsetScore.
        assertThat(resp.getBody().getEntries()).extracting(LeaderboardEntry::getUserId)
                .containsExactly("user-A", "user-X", "user-B", "user-Y");
        // totalParticipants is the sum of the two regions.
        assertThat(resp.getBody().getTotalParticipants()).isEqualTo(4L);
    }

    @Test
    void merge_userIdCollision_keepsLocalAndDropsPeer() throws IOException {
        // Data-anomaly defence: if the same userId somehow shows up in both
        // regions (misrouted submission, etc.) we keep the local row and drop
        // the peer row rather than summing or duplicating. This is the
        // "no-overlaps-in-well-formed-data" guardrail.
        when(service.getTop("c-1", 0, 10)).thenReturn(List.of(
                row(1, "user-A", 100.0, 100, 0)));
        when(service.getParticipantCount("c-1")).thenReturn(1L);

        String peerBody = mapper.writeValueAsString(Map.of(
                "contestId", "c-1",
                "totalParticipants", 1L,
                "entries", List.of(
                        Map.of("rank", 1, "userId", "user-A", "zsetScore", 999.0,
                               "points", 999, "penaltyMinutes", 0, "region", "us-central1")),
                "degraded", false));

        LeaderboardController controller = newController(
                req -> fakeResponse(200, peerBody),
                "asia-south1", "http://peer:8082");

        ResponseEntity<LeaderboardResponse> resp =
                controller.getLeaderboard("c-1", true, 0, 10);

        assertThat(resp.getBody().getEntries()).hasSize(1);
        LeaderboardEntry only = resp.getBody().getEntries().get(0);
        assertThat(only.getUserId()).isEqualTo("user-A");
        // Local row wins — score is 100, not 999.
        assertThat(only.getPoints()).isEqualTo(100L);
        assertThat(only.getRegion()).isEqualTo("asia-south1");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static LeaderboardEntry row(long rank, String userId, double zsetScore,
                                         long points, long penalty) {
        return new LeaderboardEntry(rank, userId, zsetScore, points, penalty, null);
    }

    /** Minimal {@link HttpResponse} for tests — only statusCode + body are used. */
    private static HttpResponse<String> fakeResponse(int status, String body) {
        return new HttpResponse<>() {
            @Override public int statusCode() { return status; }
            @Override public HttpRequest request() { return null; }
            @Override public Optional<HttpResponse<String>> previousResponse() { return Optional.empty(); }
            @Override public HttpHeaders headers() {
                return HttpHeaders.of(Map.of(), (a, b) -> true);
            }
            @Override public String body() { return body; }
            @Override public Optional<SSLSession> sslSession() { return Optional.empty(); }
            @Override public URI uri() { return URI.create("http://peer/api/v1/leaderboard/c-1"); }
            @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_1_1; }
        };
    }
}
