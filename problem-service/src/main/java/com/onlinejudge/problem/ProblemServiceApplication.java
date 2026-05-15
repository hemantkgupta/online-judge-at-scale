package com.onlinejudge.problem;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Problem Service — owns problem metadata and test-case catalogues.
 *
 * Reads are dominated by execution workers fetching test-case URLs for every
 * submission; writes happen only when an operator uploads a new problem (rare).
 *
 * The service stays out of the data plane: it returns V4-signed GCS URLs
 * (5-min TTL) rather than streaming bytes. See blog post
 * `raw-blog/execution-service-gcp.md` "Where the test cases come from" for
 * the architectural rationale.
 */
@SpringBootApplication
public class ProblemServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProblemServiceApplication.class, args);
    }
}
