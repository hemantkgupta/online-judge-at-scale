package com.onlinejudge.leaderboard.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.leaderboard.writer.LeaderboardWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the verdict-push consumer.
 *
 * Wire format is Protobuf (Part 2): the consumer reads {@link VerdictEvent}
 * bytes from Kafka and transcodes them to JSON before pushing to the WebSocket
 * client and caching to Redis.
 *
 * Verifies that:
 *  - When the user is connected locally, the JSON-transcoded verdict is
 *    forwarded to the user's per-user STOMP destination.
 *  - When the user is NOT connected locally, the live push is skipped — the
 *    HTTP fallback ({@code GET /api/v1/submissions/{id}/verdict}) covers
 *    reconnect-resume.
 *  - The JSON-transcoded verdict is cached in Redis under
 *    {@code verdict:{submissionId}} with a TTL <em>regardless</em> of
 *    connection state.
 *  - A verdict with no userId is dropped (no broadcast / no exception).
 *  - A malformed proto blob is dropped without throwing.
 */
@ExtendWith(MockitoExtension.class)
class VerdictPushConsumerTest {

    @Mock private SimpMessagingTemplate messagingTemplate;
    @Mock private StringRedisTemplate redisTemplate;
    @Mock private ValueOperations<String, String> valueOps;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private VerdictConnectionRegistry registry;
    private VerdictPushConsumer consumer;

    @BeforeEach
    void setUp() {
        registry = new VerdictConnectionRegistry();
        // The writer is optional (drops out via @ConditionalOnProperty when
        // app.leaderboard.writer.enabled=false). These tests exercise the
        // verdict-cache + WS push paths only; pass an empty provider.
        ObjectProvider<LeaderboardWriter> emptyWriter = emptyProvider();
        consumer = new VerdictPushConsumer(messagingTemplate, redisTemplate, objectMapper, registry, emptyWriter);
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOps);
    }

    /** Minimal {@link ObjectProvider} that resolves to no writer bean. */
    private static ObjectProvider<LeaderboardWriter> emptyProvider() {
        return new ObjectProvider<>() {
            @Override public LeaderboardWriter getObject() { throw new UnsupportedOperationException(); }
            @Override public LeaderboardWriter getObject(Object... args) { throw new UnsupportedOperationException(); }
            @Override public LeaderboardWriter getIfAvailable() { return null; }
            @Override public LeaderboardWriter getIfUnique() { return null; }
            @Override public Stream<LeaderboardWriter> stream() { return Stream.empty(); }
            @Override public Stream<LeaderboardWriter> orderedStream() { return Stream.empty(); }
        };
    }

    private static byte[] proto(VerdictEvent.Builder b) {
        return b.build().toByteArray();
    }

    @Test
    void onVerdict_userConnectedLocally_pushesAndCaches() throws Exception {
        registry.register("user-abc", "sess-1");

        byte[] payload = proto(VerdictEvent.newBuilder()
                .setSubmissionId("sub-1")
                .setUserId("user-abc")
                .setProblemId("p-1")
                .setContestId("c-1")
                .setResult("ACCEPTED")
                .setExecutionTimeMs(42)
                .setMemoryUsedMb(12)
                .setGatewayTsMs(1700000000000L)
                .setPoints(100)
                .setPhase("pretest")
                .setRegion("us-east-1"));

        consumer.onVerdict(new ConsumerRecord<>("evaluated_results", 0, 0L, "user-abc", payload));

        // WebSocket destination — captured to verify it's valid JSON with the right fields.
        ArgumentCaptor<String> wsPayload = ArgumentCaptor.forClass(String.class);
        verify(messagingTemplate).convertAndSend(eq("/topic/verdicts/user-abc"), wsPayload.capture());
        JsonNode wsJson = objectMapper.readTree(wsPayload.getValue());
        assertThat(wsJson.get("submissionId").asText()).isEqualTo("sub-1");
        assertThat(wsJson.get("result").asText()).isEqualTo("ACCEPTED");
        assertThat(wsJson.get("phase").asText()).isEqualTo("pretest");
        assertThat(wsJson.get("region").asText()).isEqualTo("us-east-1");

        // Redis cache — same JSON shape, addressed by submissionId.
        verify(valueOps).set(eq("verdict:sub-1"), eq(wsPayload.getValue()), eq(Duration.ofHours(24)));
    }

    @Test
    void onVerdict_userNotConnectedLocally_skipsPushButStillCaches() {
        byte[] payload = proto(VerdictEvent.newBuilder()
                .setSubmissionId("sub-9")
                .setUserId("user-xyz")
                .setResult("WRONG_ANSWER")
                .setPhase("pretest"));

        consumer.onVerdict(new ConsumerRecord<>("evaluated_results", 0, 0L, "user-xyz", payload));

        verify(messagingTemplate, never()).convertAndSend(any(String.class), any(Object.class));
        verify(valueOps).set(eq("verdict:sub-9"), any(String.class), eq(Duration.ofHours(24)));
    }

    @Test
    void onVerdict_missingUserId_dropsSilently() {
        byte[] payload = proto(VerdictEvent.newBuilder()
                .setSubmissionId("sub-1")
                .setResult("ACCEPTED"));

        consumer.onVerdict(new ConsumerRecord<>("evaluated_results", 0, 0L, "no-key", payload));

        verify(messagingTemplate, never()).convertAndSend(any(String.class), any(Object.class));
        verify(valueOps, never()).set(any(), any(), any(Duration.class));
    }

    @Test
    void onVerdict_missingSubmissionId_pushesButSkipsRedisCache() {
        registry.register("user-1", "sess-1");

        byte[] payload = proto(VerdictEvent.newBuilder()
                .setUserId("user-1")
                .setResult("WRONG_ANSWER")
                .setPhase("pretest"));

        consumer.onVerdict(new ConsumerRecord<>("evaluated_results", 0, 0L, "user-1", payload));

        verify(messagingTemplate).convertAndSend(eq("/topic/verdicts/user-1"), any(Object.class));
        verify(valueOps, never()).set(any(), any(), any(Duration.class));
    }

    @Test
    void onVerdict_malformedBytes_dropsSilently() {
        // Random bytes that are neither a valid proto nor JSON. The consumer
        // must log and move on (auto-commit advances the offset).
        byte[] payload = "{not-json".getBytes();

        consumer.onVerdict(new ConsumerRecord<>("evaluated_results", 0, 0L, "x", payload));

        verify(messagingTemplate, never()).convertAndSend(any(String.class), any(Object.class));
        verify(valueOps, never()).set(any(), any(), any(Duration.class));
    }
}
