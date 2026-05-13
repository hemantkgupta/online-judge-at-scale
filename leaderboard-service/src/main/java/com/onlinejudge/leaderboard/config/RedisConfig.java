package com.onlinejudge.leaderboard.config;

import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import com.onlinejudge.leaderboard.service.ScoreUpdateSubscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * Redis configuration with optional primary/replica read-write split.
 *
 * <p>Implements the leaderboard read-replica pattern from Part 9 of the blog:
 *
 * <ul>
 *   <li><b>Writes</b> (verdict-cache {@code SET}s, Pub/Sub {@code SUBSCRIBE}) go
 *       through the default {@code StringRedisTemplate}, which Spring Boot
 *       auto-configures from {@code spring.data.redis.host/port}.</li>
 *   <li><b>Reads</b> (leaderboard {@code ZREVRANGE}, {@code ZREVRANK},
 *       {@code ZSCORE}, {@code ZCARD}) go through {@code readRedisTemplate},
 *       which targets {@code app.redis.replica.host/port} when configured.</li>
 * </ul>
 *
 * <p>If no replica is configured, {@code readRedisTemplate} delegates to the
 * primary — single-node mode keeps working unchanged.
 *
 * <p>Note on Pub/Sub: the listener stays bound to the primary so we never miss
 * a {@code score_updates:*} notification due to replication lag. Redis Pub/Sub
 * does propagate to replicas, but failing closed on the primary connection is
 * simpler than tolerating replica disconnects for a delivery-critical channel.
 */
@Configuration
public class RedisConfig {

    @Value("${app.redis.replica.host:}")
    private String replicaHost;

    @Value("${app.redis.replica.port:0}")
    private int replicaPort;

    /**
     * Subscribe to all score update channels {@code score_updates:*} on the
     * <em>primary</em> connection factory (Spring Boot's auto-configured one).
     * Covers every contestId without enumerating contests at startup.
     */
    @Bean
    public RedisMessageListenerContainer redisListenerContainer(
            RedisConnectionFactory connectionFactory,
            ScoreUpdateSubscriber subscriber) {

        MessageListenerAdapter adapter = new MessageListenerAdapter(subscriber, "onMessage");

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(adapter, new PatternTopic("score_updates:*"));
        return container;
    }

    /**
     * Read-path template. If {@code app.redis.replica.host} is set, it talks to
     * the replica; otherwise it reuses the primary connection factory so
     * single-node deployments still work without configuration.
     */
    @Bean
    public StringRedisTemplate readRedisTemplate(RedisConnectionFactory primary) {
        if (replicaHost != null && !replicaHost.isBlank() && replicaPort > 0) {
            RedisStandaloneConfiguration cfg = new RedisStandaloneConfiguration(replicaHost, replicaPort);
            LettuceConnectionFactory replica = new LettuceConnectionFactory(cfg);
            replica.afterPropertiesSet();
            return new StringRedisTemplate(replica);
        }
        return new StringRedisTemplate(primary);
    }

    /**
     * Score-range shard router shared with the scoring-pipeline sink.
     * Must use the same shard boundaries on both sides so writes and reads
     * agree on which Redis key holds which user.
     */
    @Bean
    public ScoreRangeShardRouter scoreRangeShardRouter() {
        return ScoreRangeShardRouter.defaultIcpcRouter();
    }
}
