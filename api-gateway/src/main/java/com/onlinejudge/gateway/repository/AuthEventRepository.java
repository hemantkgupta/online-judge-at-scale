package com.onlinejudge.gateway.repository;

import com.onlinejudge.gateway.entity.AuthEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AuthEventRepository extends JpaRepository<AuthEvent, UUID> {
}
