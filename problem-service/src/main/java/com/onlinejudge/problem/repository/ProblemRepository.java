package com.onlinejudge.problem.repository;

import com.onlinejudge.problem.entity.Problem;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProblemRepository extends JpaRepository<Problem, UUID> {
}
