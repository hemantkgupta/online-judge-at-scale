package com.onlinejudge.leaderboard.integration;

import com.onlinejudge.leaderboard.service.VerdictConnectionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end smoke test for the verdict-push flow.
 *
 * Exercises the real wiring (no mocked SimpMessagingTemplate):
 *
 * <pre>
 *   KafkaTemplate.send("evaluated_results", verdictBytes)
 *       → VerdictPushConsumer (@KafkaListener)
 *           → SimpMessagingTemplate.convertAndSend("/topic/verdicts/{userId}", ...)
 *               → StompSubscriber receives the payload
 * </pre>
 *
 * <p>Kafka runs in-process via {@code @EmbeddedKafka} (no Docker required).
 * Redis beans are mocked out so the Spring context still wires the Pub/Sub
 * listener container; the Pub/Sub side is exercised separately by
 * {@code ScoreUpdateSubscriber} unit tests.
 */
@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
        "app.kafka.topic.evaluated-results=evaluated_results",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        // Test-only producer wiring so the embedded KafkaTemplate accepts byte[] payloads.
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
        "spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
        // Mocked Redis beans break the reactive health-check autoconfig; the smoke test
        // doesn't exercise Redis at all, so just turn that piece off.
        "management.health.redis.enabled=false",
        "spring.autoconfigure.exclude="
            + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration,"
            + "org.springframework.boot.actuate.autoconfigure.data.redis.RedisHealthContributorAutoConfiguration,"
            + "org.springframework.boot.actuate.autoconfigure.data.redis.RedisReactiveHealthContributorAutoConfiguration"
    }
)
@EmbeddedKafka(partitions = 1, topics = {"evaluated_results"})
class VerdictPushIntegrationTest {

    @LocalServerPort
    private int port;

    @MockBean
    private StringRedisTemplate stringRedisTemplate;

    @MockBean
    private RedisConnectionFactory redisConnectionFactory;

    @MockBean
    private RedisMessageListenerContainer redisMessageListenerContainer;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private VerdictConnectionRegistry connectionRegistry;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void stubRedisOpsForValue() {
        // The consumer caches every verdict to Redis (verdict:{id} → JSON) so the
        // HTTP fallback can serve it. With @MockBean, opsForValue() returns null
        // unless stubbed — return a no-op mock so the cache-then-push flow doesn't
        // NPE before the live push happens.
        ValueOperations<String, String> ops = Mockito.mock(ValueOperations.class);
        Mockito.when(stringRedisTemplate.opsForValue()).thenReturn(ops);
    }

    @DynamicPropertySource
    static void kafkaBrokers(DynamicPropertyRegistry registry) {
        // EmbeddedKafka publishes its broker list as a system property; route both
        // producers (used by the test) and the @KafkaListener consumer through it.
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void verdictOnKafka_arrivesOnPerUserStompDestination() throws Exception {
        String userId = "test-user-" + System.nanoTime();
        String destination = "/topic/verdicts/" + userId;

        // 1. Connect a STOMP client to the running server.
        WebSocketClient wsClient = new StandardWebSocketClient();
        WebSocketStompClient stomp = new WebSocketStompClient(wsClient);
        stomp.setMessageConverter(new StringMessageConverter());

        BlockingQueue<String> received = new ArrayBlockingQueue<>(8);

        StompSession session = stomp
                .connectAsync("ws://localhost:" + port + "/ws/websocket",
                        new StompSessionHandlerAdapter() {})
                .get(5, TimeUnit.SECONDS);

        // The verdict consumer gates the live push on the connection registry
        // (Part 9 co-partitioning). The interceptor in WebSocketConfig normally
        // populates this from the STOMP CONNECT `userId` header, but the SockJS
        // STOMP test client here doesn't carry custom CONNECT headers via this
        // overload; register manually so the gateway sees the user as live.
        connectionRegistry.register(userId, session.getSessionId());

        try {
            session.subscribe(destination, new StompFrameHandler() {
                @Override
                public Type getPayloadType(StompHeaders headers) {
                    return String.class;
                }

                @Override
                public void handleFrame(StompHeaders headers, Object payload) {
                    received.offer((String) payload);
                }
            });

            // 2. Publish a verdict event (proto, Part 2 wire format) to the Kafka topic.
            byte[] verdictBytes = com.onlinejudge.common.events.Events.VerdictEvent.newBuilder()
                    .setSubmissionId("sub-smoke-1")
                    .setUserId(userId)
                    .setProblemId("p-1")
                    .setContestId("c-1")
                    .setResult("ACCEPTED")
                    .setExecutionTimeMs(42)
                    .setMemoryUsedMb(12)
                    .setPhase("pretest")
                    .build()
                    .toByteArray();
            kafkaTemplate.send("evaluated_results", userId, verdictBytes).get(5, TimeUnit.SECONDS);

            // 3. The consumer should forward it to the per-user destination.
            String pushed = received.poll(10, TimeUnit.SECONDS);
            assertThat(pushed)
                    .as("Expected a verdict on %s within timeout", destination)
                    .isNotNull();
            assertThat(pushed).contains("\"submissionId\":\"sub-smoke-1\"");
            assertThat(pushed).contains("\"userId\":\"" + userId + "\"");
            assertThat(pushed).contains("\"result\":\"ACCEPTED\"");
            assertThat(pushed).contains("\"phase\":\"pretest\"");
        } finally {
            if (session.isConnected()) {
                session.disconnect();
            }
            stomp.stop();
        }
    }
}
