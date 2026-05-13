package com.onlinejudge.gateway.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Dual-scoped sliding-window rate limiter (per Part 3 of the blog).
 *
 * <p>Each submission is checked against two independent token buckets, both
 * incremented in a single atomic Lua script:
 * <ul>
 *   <li>{@code rate_limit:user:{userId}:{minuteBucket}} — per-user limit
 *       (defends against a client bug or casual abuse from a logged-in user)</li>
 *   <li>{@code rate_limit:ip:{sourceIp}:{minuteBucket}} — per-IP limit
 *       (defends against credential-stuffing and bot fleets that rotate users
 *       from the same network)</li>
 * </ul>
 *
 * <p>Atomicity matters: the script does {@code INCR} + {@code EXPIRE} (on the
 * first increment of each bucket) inside one Redis operation, so there is no
 * window between the increment and the TTL set where a key could remain without
 * an expiry. The previous two-call implementation had exactly that race.
 *
 * <p>The decision is returned as a short status string so callers can log
 * <em>which</em> bucket tripped: {@code OK}, {@code USER_LIMIT}, or {@code IP_LIMIT}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RateLimitService {

    private static final String LUA_RATE_LIMIT = """
            local user_key  = KEYS[1]
            local ip_key    = KEYS[2]
            local user_max  = tonumber(ARGV[1])
            local ip_max    = tonumber(ARGV[2])
            local ttl       = tonumber(ARGV[3])

            local user_count = redis.call('INCR', user_key)
            if user_count == 1 then
                redis.call('EXPIRE', user_key, ttl)
            end

            local ip_count = redis.call('INCR', ip_key)
            if ip_count == 1 then
                redis.call('EXPIRE', ip_key, ttl)
            end

            if user_count > user_max then return 'USER_LIMIT' end
            if ip_count   > ip_max   then return 'IP_LIMIT'   end
            return 'OK'
            """;

    private static final RedisScript<String> SCRIPT =
            new DefaultRedisScript<>(LUA_RATE_LIMIT, String.class);

    /** Per-bucket TTL — slightly longer than the bucket width so rounding never races the expiry. */
    private static final int BUCKET_TTL_SECONDS = 70;

    private final StringRedisTemplate redisTemplate;

    @Value("${app.rate-limit.submissions-per-minute:10}")
    private int submissionsPerUserPerMinute;

    @Value("${app.rate-limit.submissions-per-ip-per-minute:60}")
    private int submissionsPerIpPerMinute;

    /**
     * Checks the user and IP buckets atomically. Returns {@code true} if both buckets
     * are within their limits after this request, {@code false} otherwise.
     */
    public boolean isAllowed(String userId, String sourceIp) {
        long minuteBucket = Instant.now().getEpochSecond() / 60;
        String userKey = "rate_limit:user:" + userId + ":" + minuteBucket;
        String ipKey   = "rate_limit:ip:"   + (sourceIp == null ? "unknown" : sourceIp) + ":" + minuteBucket;

        String result = redisTemplate.execute(
                SCRIPT,
                List.of(userKey, ipKey),
                String.valueOf(submissionsPerUserPerMinute),
                String.valueOf(submissionsPerIpPerMinute),
                String.valueOf(BUCKET_TTL_SECONDS));

        if ("USER_LIMIT".equals(result)) {
            log.warn("[rate-limit] User bucket tripped: userId={} bucket={}", userId, minuteBucket);
            return false;
        }
        if ("IP_LIMIT".equals(result)) {
            log.warn("[rate-limit] IP bucket tripped: ip={} bucket={}", sourceIp, minuteBucket);
            return false;
        }
        return true;
    }
}
