package com.example.agents.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the main topic explicitly so partitions/replication are under
 * our control (rather than relying on broker auto-create defaults).
 *
 * The retry topics (gmail-messages-retry-0, -retry-1, ...) and the DLT
 * (gmail-messages-dlt) are created automatically at runtime by
 * @RetryableTopic on the consumer (see SheetsWriterAgent), using the same
 * partition count as the main topic.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${agents.kafka.topic.gmail-messages}")
    private String gmailMessagesTopic;

    @Value("${agents.kafka.topic.partitions}")
    private int partitions;

    @Value("${agents.kafka.topic.replication-factor}")
    private short replicationFactor;

    @Bean
    public NewTopic gmailMessagesTopic() {
        return TopicBuilder.name(gmailMessagesTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}
