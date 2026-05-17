package com.onlinejudge.contest.model;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JPA entity backing the {@code contest_problems} join table (Flyway V7).
 *
 * Many-to-many between contests and problems:
 *   * a contest has many problems (typical: 6-8 per round)
 *   * a problem can appear in multiple contests (re-use across practice rounds)
 *
 * Contest-service is read-only against this table for now — the api-gateway
 * admin endpoints own writes. We model it as a full JPA entity (rather than a
 * native-query-only relation) so the repository can grow alongside future
 * features like per-contest point overrides.
 */
@Entity
@Table(name = "contest_problems")
@Data
@NoArgsConstructor
public class ContestProblem {

    @EmbeddedId
    private ContestProblemId id;

    /** 1-based problem ordering within the contest (A=1, B=2, …). */
    @Column(name = "ordinal", nullable = false)
    private int ordinal;

    /** Points awarded for solving this problem in this contest. */
    @Column(name = "points", nullable = false)
    private int points;
}
