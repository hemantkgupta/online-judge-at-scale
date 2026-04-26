package com.onlinejudge.gateway.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic.pretest}")
    private String pretestTopic;

    @Value("${app.kafka.topic.system}")
    private String systemTopic;

    // Partitioned by userId hash - 12 partitions gives good parallelism locally
    @Bean
    public NewTopic pretestTopic() {
        return TopicBuilder.name(pretestTopic)
                .partitions(12)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic systemTopic() {
        return TopicBuilder.name(systemTopic)
                .partitions(12)
                .replicas(1)
                .build();
    }
}
