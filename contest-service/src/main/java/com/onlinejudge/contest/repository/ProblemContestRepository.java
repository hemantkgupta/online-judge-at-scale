package com.onlinejudge.contest.repository;

import com.onlinejudge.contest.model.ContestProblem;
import com.onlinejudge.contest.model.ContestProblemId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Read-only access to the {@code contest_problems} join table introduced in
 * Flyway V7. Used by {@code SystemTestReplayPublisher} to enumerate the
 * problems that belong to a given contest before walking their ACCEPTED
 * pretest submissions for re-publication on {@code submissions.system}.
 *
 * Contest-service does not own writes to {@code contest_problems} — admins
 * insert rows via api-gateway endpoints. This repository only reads.
 *
 * The native query avoids hydrating the {@link ContestProblem} entity when
 * all we need are the problem IDs in display order; full entities are still
 * available through inherited {@code JpaRepository} methods if needed later.
 */
@Repository
public interface ProblemContestRepository extends JpaRepository<ContestProblem, ContestProblemId> {

    /**
     * Returns the problem IDs that belong to the given contest, in display
     * order (ordinal ascending). Drives the system-test replay scan.
     */
    @Query(value = """
            SELECT problem_id
              FROM contest_problems
             WHERE contest_id = :contestId
             ORDER BY ordinal ASC
            """, nativeQuery = true)
    List<UUID> findProblemIdsByContestId(@Param("contestId") UUID contestId);
}
