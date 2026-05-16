package com.onlinejudge.gateway.dto;

import lombok.Data;

/**
 * Pair of access + refresh tokens issued by login / refresh.
 *
 * <p>{@code expiresIn} is the access-token lifetime in seconds (matches the
 * OAuth 2 convention so clients can plug this in without surprise).
 */
@Data
public class TokenPairResponse {
    private final String accessToken;
    private final String refreshToken;
    private final long expiresIn;
}
