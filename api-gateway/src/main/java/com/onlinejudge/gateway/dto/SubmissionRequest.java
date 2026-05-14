package com.onlinejudge.gateway.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class SubmissionRequest {

    // No userId field — the authenticated identity is the JWT subject set by
    // JwtAuthenticationFilter (Part 3 of the blog). The controller passes
    // that principal to the service explicitly; trusting a userId in the
    // request body would let any caller claim any account.

    @NotBlank
    private String problemId;

    private String contestId;

    @NotBlank
    private String language;

    @NotBlank
    @Size(max = 65536, message = "Code too large (max 64 KB)")
    private String code;
}
