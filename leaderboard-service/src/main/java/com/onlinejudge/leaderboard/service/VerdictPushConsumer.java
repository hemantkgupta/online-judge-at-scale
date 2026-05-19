package com.onlinejudge.leaderboard.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.leaderboard.writer.LeaderboardWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Verdict push: regional Kafka → per-user WebSocket destination.
 *
 * <p>Implements the gateway role of Part 9's Push Service. Verdicts arrive on
 * the {@code evaluated_results} Kafka topic as <b>Protobuf {@link VerdictEvent}</b>
 * messages (Part 2). The browser still receives JSON over the STOMP WebSocket —
 * the proto is transcoded to a flat JSON object on the way out so the
 * client-side parsing stays simple.
 *
 * <p>With co-partitioned routing (see
 * {@link com.onlinejudge.common.sharding.PartitionAssigner}) the WebSocket LB
 * and the Kafka consumer-group rebalancer agree on which gateway instance
 * owns each user, so the verdict and the live WebSocket land on the
 * <em>same</em> instance.
 *
 * <p>Before broadcasting, this consumer checks
 * {@link VerdictConnectionRegistry#isConnectedLocally(String)}:
 * <ul>
 *   <li><b>Connected here</b> → fast path: forward the JSON-shaped verdict to
 *       {@code /topic/verdicts/{userId}} so the client gets it immediately.</li>
 *   <li><b>Not connected here</b> → skip the STOMP send. The user's reconnect
 *       (or first-time poll) will hit
 *       {@code GET /api/v1/submissions/{id}/verdict} and read the verdict
 *       from Redis instead.</li>
 * </ul>
 *
 * <p>The Redis cache write happens <em>regardless</em> of connection state.
 */
@Slf4j
@Component
public class VerdictPushConsumer {

    private static final String VERDICT_TOPIC_PREFIX = "/topic/verdicts/";
    private static final String VERDICT_STORE_KEY_PREFIX = "verdict:";
    private static final Duration VERDICT_TTL = Duration.ofHours(24);

    private final SimpMessagingTemplate messagingTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final VerdictConnectionRegistry connectionRegistry;
    /**
     * Optional stand-in leaderboard writer. Absent when
     * {@code app.leaderboard.writer.enabled=false} — at that point Flink
     * (scoring-pipeline) owns the writes and we deliberately don't double-write.
     * Injected as {@link ObjectProvider} so the {@code @KafkaListener} bean
     * still wires (and verdict-cache + WebSocket push still run) when the
     * writer is off.
     */
    private final ObjectProvider<LeaderboardWriter> writerProvider;

    public VerdictPushConsumer(SimpMessagingTemplate messagingTemplate,
                               StringRedisTemplate redisTemplate,
                               ObjectMapper objectMapper,
                               VerdictConnectionRegistry connectionRegistry,
                               ObjectProvider<LeaderboardWriter> writerProvider) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.connectionRegistry = connectionRegistry;
        this.writerProvider = writerProvider;
    }

    // Group is region-namespaced so two regions' leaderboard-services do NOT
    // share a consumer group. If they did, Kafka would partition-rebalance
    // across regions and the verdict for a user in region-A could land in
    // region-B's leaderboard-service, which has no WebSocket session for that
    // user — silently broken. Sticky-region routing requires sticky-region
    // consumer groups. The topic is also per-region (see application.yml).
    @KafkaListener(
            topics = "${app.kafka.topic.evaluated-results}",
            groupId = "${app.kafka.consumer-group:leaderboard-verdict-push}-${app.region}"
    )
    public void onVerdict(ConsumerRecord<String, byte[]> record) {
        try {
            VerdictEvent verdict = VerdictEvent.parseFrom(record.value());
            String userId       = verdict.getUserId();
            String submissionId = verdict.getSubmissionId();
            if (userId.isEmpty()) {
                log.warn("[verdict-push] Verdict has no userId; dropping. submissionId={}",
                        submissionId.isEmpty() ? "?" : submissionId);
                return;
            }

            // Transcode proto → JSON for the WebSocket / HTTP-fallback consumers.
            // The browser sees the same JSON shape it always did; only the
            // Kafka wire format changed (Part 2 closure).
            ObjectNode json = objectMapper.createObjectNode();
            json.put("submissionId", submissionId);
            json.put("userId", userId);
            json.put("problemId", verdict.getProblemId());
            json.put("contestId", verdict.getContestId());
            json.put("result", verdict.getResult());
            json.put("executionTimeMs", verdict.getExecutionTimeMs());
            json.put("memoryUsedMb", verdict.getMemoryUsedMb());
            json.put("gatewayTsMs", verdict.getGatewayTsMs());
            json.put("points", verdict.getPoints());
            json.put("phase", verdict.getPhase().isEmpty() ? "pretest" : verdict.getPhase());
            json.put("region", verdict.getRegion());
            String payload = objectMapper.writeValueAsString(json);

            // Cache the verdict in Redis first so the HTTP fallback has it
            // regardless of whether we push live. Cache-then-push avoids a
            // race where the client reconnects, hits the fallback, and sees
            // "not found" between the push and the cache write.
            if (!submissionId.isEmpty()) {
                redisTemplate.opsForValue().set(
                        VERDICT_STORE_KEY_PREFIX + submissionId,
                        payload,
                        VERDICT_TTL);
            }

            // Stand-in leaderboard write (M1 closure; flipped off when
            // scoring-pipeline / Flink takes over via
            // app.leaderboard.writer.enabled=false). Failures here must NOT
            // break the WebSocket push: scored verdicts are eventually
            // reconciled by Flink, but a missed WS push means a stale UI for
            // the contestant.
            LeaderboardWriter writer = writerProvider.getIfAvailable();
            if (writer != null) {
                try {
                    writer.onVerdict(verdict);
                } catch (Exception ex) {
                    log.warn("[verdict-push] leaderboard writer failed (continuing with WS push). submissionId={} err={}",
                            submissionId.isEmpty() ? "?" : submissionId, ex.toString());
                }
            }

            if (!connectionRegistry.isConnectedLocally(userId)) {
                log.debug("[verdict-push] User {} not connected locally; skipping push (HTTP fallback covers reconnect). submissionId={}",
                        userId, submissionId.isEmpty() ? "?" : submissionId);
                return;
            }

            String destination = VERDICT_TOPIC_PREFIX + userId;
            messagingTemplate.convertAndSend(destination, payload);

            log.debug("[verdict-push] Pushed verdict to {} submissionId={} phase={}",
                    destination,
                    submissionId.isEmpty() ? "?" : submissionId,
                    verdict.getPhase());
        } catch (Exception ex) {
            log.error("[verdict-push] Failed to push verdict: {}", ex.getMessage(), ex);
        }
    }
}
