package com.onlinejudge.gateway.repository;

import com.onlinejudge.gateway.entity.RefreshToken;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface RefreshTokenRepository extends JpaRepository<RefreshToken, String> {

    Optional<RefreshToken> findByTokenHash(String tokenHash);

    @Modifying
    @Query("update RefreshToken rt set rt.revokedAt = :now where rt.tokenHash = :hash and rt.revokedAt is null")
    int revoke(@Param("hash") String hash, @Param("now") Instant now);

    @Modifying
    @Query("update RefreshToken rt set rt.revokedAt = :now where rt.userId = :userId and rt.revokedAt is null")
    int revokeAllForUser(@Param("userId") UUID userId, @Param("now") Instant now);
}
