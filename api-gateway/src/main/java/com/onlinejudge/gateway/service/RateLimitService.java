package com.onlinejudge.gateway.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

/**
 * Sliding window rate limiter using Redis.
 *
 * Key: rate_limit:submit:{userId}:{minuteBucket}
 * Each key holds a counter for that minute bucket.
 * TTL is 60 seconds so stale buckets auto-expire.
 */
@Service
@RequiredArgsConstructor
public class RateLimitService {

    private final StringRedisTemplate redisTemplate;

    @Value("${app.rate-limit.submissions-per-minute:10}")
    private int submissionsPerMinute;

    public boolean isAllowed(String userId) {
        long minuteBucket = Instant.now().getEpochSecond() / 60;
        String key = "rate_limit:submit:" + userId + ":" + minuteBucket;

        Long count = redisTemplate.opsForValue().increment(key);
        if (count == 1) {
            // First request in this bucket - set TTL
            redisTemplate.expire(key, Duration.ofSeconds(70));
        }

        return count <= submissionsPerMinute;
    }
}
