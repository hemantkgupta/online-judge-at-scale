package com.onlinejudge.leaderboard.config;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for the read/write template split in {@link RedisConfig} — the
 * mechanism that backs the Part 9 "regional read replica" pattern locally.
 *
 * <p>Single-node mode (no replica configured) must keep working: the read
 * template just reuses the primary connection factory. With a replica
 * configured, the read template targets the replica.
 */
class RedisConfigTest {

    @Test
    void readRedisTemplate_noReplicaConfigured_reusesPrimaryConnectionFactory() {
        RedisConfig config = new RedisConfig();
        ReflectionTestUtils.setField(config, "replicaHost", "");
        ReflectionTestUtils.setField(config, "replicaPort", 0);
        RedisConnectionFactory primary = mock(RedisConnectionFactory.class);

        StringRedisTemplate t = config.readRedisTemplate(primary);

        assertThat(t.getConnectionFactory())
                .as("With no replica configured, read template should reuse the primary CF.")
                .isSameAs(primary);
    }

    @Test
    void readRedisTemplate_replicaConfigured_targetsReplicaHostAndPort() {
        RedisConfig config = new RedisConfig();
        ReflectionTestUtils.setField(config, "replicaHost", "replica.local");
        ReflectionTestUtils.setField(config, "replicaPort", 6380);
        RedisConnectionFactory primary = mock(RedisConnectionFactory.class);

        StringRedisTemplate t = config.readRedisTemplate(primary);

        assertThat(t.getConnectionFactory())
                .as("With a replica configured, read template must NOT reuse the primary CF.")
                .isNotSameAs(primary);

        LettuceConnectionFactory replicaCf = (LettuceConnectionFactory) t.getConnectionFactory();
        assertThat(replicaCf.getHostName()).isEqualTo("replica.local");
        assertThat(replicaCf.getPort()).isEqualTo(6380);
    }

    @Test
    void readRedisTemplate_blankReplicaHost_reusesPrimary() {
        // Empty string should also fall through to primary — handles the
        // ${REDIS_REPLICA_HOST:} default-empty behavior in application.yml.
        RedisConfig config = new RedisConfig();
        ReflectionTestUtils.setField(config, "replicaHost", "   ");
        ReflectionTestUtils.setField(config, "replicaPort", 6380);
        RedisConnectionFactory primary = mock(RedisConnectionFactory.class);

        StringRedisTemplate t = config.readRedisTemplate(primary);

        assertThat(t.getConnectionFactory()).isSameAs(primary);
    }

    @Test
    void readRedisTemplate_replicaPortZero_reusesPrimary() {
        // Port=0 means "unset"; the helper must fall through to primary.
        RedisConfig config = new RedisConfig();
        ReflectionTestUtils.setField(config, "replicaHost", "replica.local");
        ReflectionTestUtils.setField(config, "replicaPort", 0);
        RedisConnectionFactory primary = mock(RedisConnectionFactory.class);

        StringRedisTemplate t = config.readRedisTemplate(primary);

        assertThat(t.getConnectionFactory()).isSameAs(primary);
    }
}
