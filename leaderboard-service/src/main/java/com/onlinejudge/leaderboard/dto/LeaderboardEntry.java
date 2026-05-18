package com.onlinejudge.leaderboard.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A single row on the leaderboard. Wire shape used by both the local-only and
 * the global (cross-region) read paths.
 *
 * <p>{@code region} is non-null on the global path so the caller can see which
 * region each user "lives" in. On the local path it equals the serving
 * region; on the global path it's whatever region returned the row.
 *
 * <p>{@code rank} is 1-indexed. On the global path it's the merged-and-sliced
 * rank across both regions; on the local path it's the rank within the serving
 * region only.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LeaderboardEntry {

    private long rank;
    private String userId;
    private Double zsetScore;
    private long points;
    private long penaltyMinutes;
    private String region;

    public LeaderboardEntry() {
        // Jackson
    }

    public LeaderboardEntry(long rank, String userId, Double zsetScore,
                            long points, long penaltyMinutes, String region) {
        this.rank = rank;
        this.userId = userId;
        this.zsetScore = zsetScore;
        this.points = points;
        this.penaltyMinutes = penaltyMinutes;
        this.region = region;
    }

    public LeaderboardEntry withRegion(String region) {
        return new LeaderboardEntry(rank, userId, zsetScore, points, penaltyMinutes, region);
    }

    public LeaderboardEntry withRank(long newRank) {
        return new LeaderboardEntry(newRank, userId, zsetScore, points, penaltyMinutes, region);
    }

    public long getRank() { return rank; }
    public void setRank(long rank) { this.rank = rank; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public Double getZsetScore() { return zsetScore; }
    public void setZsetScore(Double zsetScore) { this.zsetScore = zsetScore; }

    public long getPoints() { return points; }
    public void setPoints(long points) { this.points = points; }

    public long getPenaltyMinutes() { return penaltyMinutes; }
    public void setPenaltyMinutes(long penaltyMinutes) { this.penaltyMinutes = penaltyMinutes; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
}
