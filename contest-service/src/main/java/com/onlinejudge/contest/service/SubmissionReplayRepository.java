package com.onlinejudge.contest.service;

import com.onlinejudge.contest.dto.AcceptedSubmissionRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Native-SQL repository for ACCEPTED pretest submissions, used by the
 * system-test replay path (Roadmap §4.23).
 *
 * Submissions live in the unified {@code onlinejudge} database alongside
 * contests (post-V3 unification), so contest-service can read them directly
 * with the shared datasource. We avoid mapping the submission as a full JPA
 * entity here — the gateway owns the {@code Submission} entity and its
 * lifecycle, and a parallel JPA entity in contest-service would invite
 * Hibernate schema-validation conflicts. JdbcTemplate sidesteps that.
 *
 * The query is paginated by (created_at, id) to give a stable order across
 * pages without an OFFSET that would re-scan increasingly large prefixes on
 * a hot contest.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class SubmissionReplayRepository {

    private final JdbcTemplate jdbc;

    /** Page size for the replay scan. Caller pages until {@link #pageAccepted} returns less than this. */
    public static final int PAGE_SIZE = 1000;

    /**
     * Returns one page of ACCEPTED submissions for the given problem, ordered
     * by (created_at, id) ascending. Pass {@code null} cursors for the first
     * page; pass the last row's {@code createdAtMs} and {@code id} for the
     * next page.
     */
    public List<AcceptedSubmissionRow> pageAccepted(UUID problemId,
                                                    Long cursorCreatedAtMs,
                                                    UUID cursorId,
                                                    int limit) {
        // We need a stable cursor pair. created_at is a TIMESTAMP — we coerce
        // to epoch-millis on both sides so callers can carry a Long cursor
        // without dragging Instant arithmetic across the boundary.
        String sql;
        Object[] args;
        if (cursorCreatedAtMs == null || cursorId == null) {
            sql = """
                    SELECT id, user_id, problem_id, contest_id, language,
                           s3_code_url, gateway_ts_ms, region,
                           (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms
                      FROM submissions
                     WHERE problem_id = ?
                       AND status = 'ACCEPTED'
                     ORDER BY created_at ASC, id ASC
                     LIMIT ?
                    """;
            args = new Object[]{problemId, limit};
        } else {
            sql = """
                    SELECT id, user_id, problem_id, contest_id, language,
                           s3_code_url, gateway_ts_ms, region,
                           (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms
                      FROM submissions
                     WHERE problem_id = ?
                       AND status = 'ACCEPTED'
                       AND ( (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT > ?
                          OR ((EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT = ? AND id > ?) )
                     ORDER BY created_at ASC, id ASC
                     LIMIT ?
                    """;
            args = new Object[]{problemId, cursorCreatedAtMs, cursorCreatedAtMs, cursorId, limit};
        }

        return jdbc.query(sql, (rs, rowNum) -> new AcceptedSubmissionRow(
                (UUID) rs.getObject("id"),
                (UUID) rs.getObject("user_id"),
                (UUID) rs.getObject("problem_id"),
                (UUID) rs.getObject("contest_id"),
                rs.getString("language"),
                rs.getString("s3_code_url"),
                rs.getLong("gateway_ts_ms"),
                rs.getString("region"),
                rs.getLong("created_at_ms")
        ), args);
    }
}
