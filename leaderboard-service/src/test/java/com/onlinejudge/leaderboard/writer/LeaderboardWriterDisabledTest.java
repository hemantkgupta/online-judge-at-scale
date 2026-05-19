package com.onlinejudge.leaderboard.writer;

import com.onlinejudge.leaderboard.service.VerdictPushConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code app.leaderboard.writer.enabled=false} actually drops the
 * {@link LeaderboardWriter} bean from the context — the cutover precondition.
 *
 * <p>The {@link VerdictPushConsumer} must still wire (so the verdict-cache +
 * WebSocket-push paths keep running) when the writer is off. We assert both:
 * the writer bean is absent, the consumer bean is present.
 *
 * <p>This test boots the Spring context only — no embedded Kafka, no Redis. The
 * point is purely to prove the conditional-bean wiring; not the runtime behaviour.
 */
@SpringBootTest(
        properties = {
                "app.leaderboard.writer.enabled=false",
                // The @KafkaListener won't actually connect; we only want the
                // bean to instantiate so we can assert it exists.
                "spring.kafka.bootstrap-servers=localhost:9092",
                "spring.kafka.consumer.auto-offset-reset=latest",
                // We're not exercising the Kafka listener; don't autostart it
                // (no broker available in this unit-test JVM).
                "spring.kafka.listener.auto-startup=false",
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration,"
                        + "org.springframework.boot.actuate.autoconfigure.data.redis.RedisHealthContributorAutoConfiguration,"
                        + "org.springframework.boot.actuate.autoconfigure.data.redis.RedisReactiveHealthContributorAutoConfiguration",
                "management.health.redis.enabled=false",
                "management.health.kafka.enabled=false"
        }
)
@TestPropertySource(properties = "app.kafka.topic.evaluated-results=evaluated_results")
class LeaderboardWriterDisabledTest {

    @MockBean private StringRedisTemplate stringRedisTemplate;
    @MockBean private RedisConnectionFactory redisConnectionFactory;
    @MockBean private RedisMessageListenerContainer redisMessageListenerContainer;

    @Autowired private ApplicationContext context;

    @Test
    void writerBean_absentWhenFlagOff_consumerStillWired() {
        // The writer must NOT be in the context — Flink owns the writes once
        // the flag is flipped.
        assertThat(context.getBeansOfType(LeaderboardWriter.class)).isEmpty();
        // The verdict-push pipeline (cache + WS push) must still wire so the
        // reader/push paths keep working post-cutover.
        assertThat(context.getBean(VerdictPushConsumer.class)).isNotNull();
    }
}
