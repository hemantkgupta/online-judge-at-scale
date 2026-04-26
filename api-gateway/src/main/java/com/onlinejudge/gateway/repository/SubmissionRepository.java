package com.onlinejudge.gateway.repository;

import com.onlinejudge.gateway.model.Submission;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface SubmissionRepository extends JpaRepository<Submission, UUID> {
}
