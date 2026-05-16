package com.onlinejudge.gateway.dto;

import lombok.Data;

import java.util.UUID;

@Data
public class SignupResponse {
    private final UUID userId;
}
