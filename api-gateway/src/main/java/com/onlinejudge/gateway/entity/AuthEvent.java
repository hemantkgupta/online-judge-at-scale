package com.onlinejudge.gateway.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * Audit row for one authentication-related action (signup, login, refresh, logout,
 * or a failure thereof).
 */
@Entity
@Table(name = "auth_events")
@Getter
@Setter
@NoArgsConstructor
public class AuthEvent {

    @Id
    @Column(nullable = false)
    private UUID id;

    /** Nullable: failed signups / unknown-username logins have no user_id yet. */
    @Column(name = "user_id")
    private UUID userId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "ip_address")
    private String ipAddress;

    @Column(name = "user_agent")
    private String userAgent;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column
    private String detail;
}
