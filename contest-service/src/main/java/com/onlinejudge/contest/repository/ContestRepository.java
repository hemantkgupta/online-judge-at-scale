package com.onlinejudge.contest.repository;

import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.model.ContestState;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ContestRepository extends JpaRepository<Contest, UUID> {

    /** Finds REGISTRATION contests whose start time has passed (need ACTIVE transition). */
    List<Contest> findByStateAndStartTimeBefore(ContestState state, Instant time);

    /** Finds ACTIVE contests whose end time has passed (need CLOSED transition). */
    List<Contest> findByStateAndEndTimeBefore(ContestState state, Instant time);

    /** Finds all contests in a given state. */
    List<Contest> findByState(ContestState state);
}
