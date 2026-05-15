package com.onlinejudge.problem.repository;

import com.onlinejudge.problem.entity.TestCase;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TestCaseRepository extends JpaRepository<TestCase, UUID> {

    List<TestCase> findByProblemIdOrderByOrdinal(UUID problemId);

    List<TestCase> findByProblemIdAndOrdinalLessThanEqualOrderByOrdinal(UUID problemId, int maxOrdinal);
}
