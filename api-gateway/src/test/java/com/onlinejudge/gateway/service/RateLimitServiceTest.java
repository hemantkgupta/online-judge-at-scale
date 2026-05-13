package com.onlinejudge.gateway.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the dual-scoped (user + IP) atomic-Lua rate limiter.
 *
 * Verifies:
 *  - The Lua script is executed via Spring's RedisTemplate.execute(...)
 *  - Both buckets ({@code rate_limit:user:...}, {@code rate_limit:ip:...}) are passed as KEYS
 *  - {@code OK} → request allowed; {@code USER_LIMIT} / {@code IP_LIMIT} → request rejected
 *  - {@code X-Forwarded-For}-aware IP handling is NOT in this service; the controller is
 *    responsible for resolving the client IP. Here we just verify the service treats the
 *    string argument as opaque.
 */
@ExtendWith(MockitoExtension.class)
class RateLimitServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @InjectMocks
    private RateLimitService rateLimitService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(rateLimitService, "submissionsPerUserPerMinute", 10);
        ReflectionTestUtils.setField(rateLimitService, "submissionsPerIpPerMinute", 60);
    }

    @Test
    void isAllowed_returnsTrue_whenBothBucketsUnderLimit() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("OK");

        assertThat(rateLimitService.isAllowed("user-1", "10.0.0.1")).isTrue();
    }

    @Test
    void isAllowed_returnsFalse_whenUserLimitTripped() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("USER_LIMIT");

        assertThat(rateLimitService.isAllowed("user-1", "10.0.0.1")).isFalse();
    }

    @Test
    void isAllowed_returnsFalse_whenIpLimitTripped() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("IP_LIMIT");

        assertThat(rateLimitService.isAllowed("user-1", "10.0.0.1")).isFalse();
    }

    @Test
    void isAllowed_keysIncludeBothUserAndIpBuckets() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("OK");

        rateLimitService.isAllowed("user-abc", "203.0.113.42");

        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(redisTemplate).execute(any(RedisScript.class), captor.capture(),
                any(), any(), any());

        List<String> keys = captor.getValue();
        assertThat(keys).hasSize(2);
        assertThat(keys.get(0)).startsWith("rate_limit:user:user-abc:");
        assertThat(keys.get(1)).startsWith("rate_limit:ip:203.0.113.42:");
    }

    @Test
    void isAllowed_argsCarryConfiguredLimitsAndTtl() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("OK");

        rateLimitService.isAllowed("u", "1.2.3.4");

        verify(redisTemplate).execute(any(RedisScript.class), anyList(),
                eqStr("10"), eqStr("60"), eqStr("70"));
    }

    @Test
    void isAllowed_nullSourceIp_substitutesPlaceholder() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("OK");

        rateLimitService.isAllowed("user-1", null);

        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(redisTemplate).execute(any(RedisScript.class), captor.capture(),
                any(), any(), any());
        assertThat(captor.getValue().get(1)).contains("unknown");
    }

    private static String eqStr(String s) {
        return org.mockito.ArgumentMatchers.eq(s);
    }
}
