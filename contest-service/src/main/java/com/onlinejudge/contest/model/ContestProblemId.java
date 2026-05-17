package com.onlinejudge.contest.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

/**
 * Composite primary key for the {@code contest_problems} join table.
 *
 * Roadmap §4.23: contest-service reads this table to enumerate problems for
 * system-test replay. The key is (contest_id, problem_id) — the natural pair
 * that prevents a problem from being enrolled twice in the same contest.
 */
@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ContestProblemId implements Serializable {

    @Column(name = "contest_id", nullable = false)
    private UUID contestId;

    @Column(name = "problem_id", nullable = false)
    private UUID problemId;
}
