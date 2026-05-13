package com.onlinejudge.leaderboard.config;

import com.onlinejudge.leaderboard.service.VerdictConnectionRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Slf4j
@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /** STOMP header carrying the user-id on CONNECT. The browser sets this after auth. */
    private static final String USER_ID_HEADER = "userId";

    private final VerdictConnectionRegistry connectionRegistry;

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // /topic — broadcast destinations (leaderboard deltas, per-user verdict topics).
        // The verdict-push consumer targets /topic/verdicts/{userId} so each contestant
        // subscribes to their own destination after the WebSocket handshake.
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Clients connect to ws://localhost:8082/ws
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }

    /**
     * Hook the registry into the inbound STOMP channel so CONNECT/DISCONNECT
     * frames keep the {@code userId → sessionId} map current. The Push Service
     * consults this map before broadcasting a verdict — if the user isn't
     * connected to <em>this</em> instance, the push is skipped and the client's
     * HTTP fallback ({@code GET /api/v1/submissions/{id}/verdict}) picks up the
     * cached verdict from Redis instead.
     */
    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(new ChannelInterceptor() {
            @Override
            public Message<?> preSend(Message<?> message, MessageChannel channel) {
                StompHeaderAccessor accessor =
                        MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
                if (accessor == null) {
                    return message;
                }
                StompCommand cmd = accessor.getCommand();
                String sessionId = accessor.getSessionId();
                if (StompCommand.CONNECT.equals(cmd)) {
                    String userId = accessor.getFirstNativeHeader(USER_ID_HEADER);
                    if (userId == null || userId.isBlank()) {
                        log.debug("[ws] CONNECT without userId header (session={})", sessionId);
                    } else {
                        connectionRegistry.register(userId, sessionId);
                    }
                } else if (StompCommand.DISCONNECT.equals(cmd)) {
                    connectionRegistry.unregisterBySession(sessionId);
                }
                return message;
            }
        });
    }
}
