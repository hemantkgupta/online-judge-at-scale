package com.onlinejudge.leaderboard.config;

import com.onlinejudge.leaderboard.service.ScoreUpdateSubscriber;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

@Configuration
public class RedisConfig {

    /**
     * Subscribe to all score update channels: "score_updates:*"
     * Covers every contestId without needing to know them in advance.
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
}
