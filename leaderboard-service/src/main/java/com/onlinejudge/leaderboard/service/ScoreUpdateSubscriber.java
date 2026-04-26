package com.onlinejudge.leaderboard.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis Pub/Sub subscriber for score-change events.
 *
 * The Flink scoring pipeline publishes to "score_updates:{contestId}"
 * after every ZADD. This subscriber receives those events and pushes
 * them to WebSocket clients subscribed to /topic/leaderboard/{contestId}.
 *
 * Differential push: only the changed rows are sent (userId, newScore, newRank),
 * not the full leaderboard. At 277 verdicts/sec, 277 small messages are pushed.
 * Each client renders only the rows visible in their viewport.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScoreUpdateSubscriber implements MessageListener {

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String channel = new String(message.getChannel());
            String body    = new String(message.getBody());

            // channel = "score_updates:{contestId}"
            String contestId = channel.substring("score_updates:".length());

            JsonNode update = objectMapper.readTree(body);

            // Push differential update to WebSocket clients for this contest
            String destination = "/topic/leaderboard/" + contestId;
            messagingTemplate.convertAndSend(destination, body);

            log.debug("[ws-push] Pushed score update to {} for user={}",
                    destination, update.get("userId").asText());

        } catch (Exception ex) {
            log.error("[ws-push] Error handling Pub/Sub message: {}", ex.getMessage());
        }
    }
}
