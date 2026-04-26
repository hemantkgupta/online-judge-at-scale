package com.onlinejudge.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AnalyticsPipelineApplication {
    public static void main(String[] args) {
        SpringApplication.run(AnalyticsPipelineApplication.class, args);
    }
}
