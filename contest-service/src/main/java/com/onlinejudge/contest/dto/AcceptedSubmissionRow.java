package com.onlinejudge.contest.dto;

import java.util.UUID;

/**
 * Row projection of an ACCEPTED submission, used by the system-test replay
 * publisher (Roadmap §4.23).
 *
 * Only the fields needed to reconstruct a {@code SubmissionEvent} for phase
 * = "system" are carried — we avoid coupling contest-service to the full
 * {@code Submission} JPA entity owned by api-gateway.
 *
 * {@code contestId} is nullable because pre-contest submissions in the same
 * problem space (practice mode) have no contest. Those rows still get
 * surfaced by the scan because we filter by problem_id, not contest_id —
 * the caller is responsible for emitting them with the *replayed* contest's
 * UUID, not the original one.
 */
public record AcceptedSubmissionRow(
        UUID id,
        UUID userId,
        UUID problemId,
        UUID contestId,
        String language,
        String s3CodeUrl,
        long gatewayTsMs,
        String region,
        long createdAtMs
) {}
