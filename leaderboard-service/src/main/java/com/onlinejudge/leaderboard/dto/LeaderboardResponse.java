package com.onlinejudge.leaderboard.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Wire envelope for both {@code /api/v1/leaderboard/{contestId}} variants:
 * the local-only path and the global (cross-region merged) path.
 *
 * <p>{@code degraded} is set when the global path could not reach the peer
 * region and is therefore returning local-region data only. Pair this with
 * the {@code X-Peer-Region-Unreachable: true} response header on the same
 * call so older clients that ignore the JSON flag still get a signal.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LeaderboardResponse {

    private String contestId;
    private int page;
    private int pageSize;
    private long totalParticipants;
    private List<LeaderboardEntry> entries;
    private boolean degraded;

    public LeaderboardResponse() {
        // Jackson
    }

    public LeaderboardResponse(String contestId, int page, int pageSize,
                               long totalParticipants, List<LeaderboardEntry> entries,
                               boolean degraded) {
        this.contestId = contestId;
        this.page = page;
        this.pageSize = pageSize;
        this.totalParticipants = totalParticipants;
        this.entries = entries;
        this.degraded = degraded;
    }

    public String getContestId() { return contestId; }
    public void setContestId(String contestId) { this.contestId = contestId; }

    public int getPage() { return page; }
    public void setPage(int page) { this.page = page; }

    public int getPageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }

    public long getTotalParticipants() { return totalParticipants; }
    public void setTotalParticipants(long totalParticipants) { this.totalParticipants = totalParticipants; }

    public List<LeaderboardEntry> getEntries() { return entries; }
    public void setEntries(List<LeaderboardEntry> entries) { this.entries = entries; }

    public boolean isDegraded() { return degraded; }
    public void setDegraded(boolean degraded) { this.degraded = degraded; }
}
