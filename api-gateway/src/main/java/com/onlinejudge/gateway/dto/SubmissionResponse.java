package com.onlinejudge.gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SubmissionResponse {
    private String submissionId;
    private String status;
    private long gatewayTsMs;
    private String message;
}
