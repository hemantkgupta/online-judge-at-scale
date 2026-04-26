package com.onlinejudge.gateway.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class SubmissionRequest {

    @NotBlank
    private String userId;

    @NotBlank
    private String problemId;

    private String contestId;

    @NotBlank
    private String language;

    @NotBlank
    @Size(max = 65536, message = "Code too large (max 64 KB)")
    private String code;
}
