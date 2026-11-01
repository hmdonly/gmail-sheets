package com.example.agents.kafka;

import com.example.agents.model.GmailMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Thin wrapper around KafkaTemplate. Uses the Gmail message ID as the Kafka
 * message key, so all retries/redeliveries of the same email stay ordered
 * on the same partition.
 */
@Component
public class GmailEventProducer {

    private static final Logger log = LoggerFactory.getLogger(GmailEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public GmailEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                               @Value("${agents.kafka.topic.gmail-messages}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(GmailMessageEvent event) {
        kafkaTemplate.send(topic, event.messageId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event for messageId={}", event.messageId(), ex);
                    } else {
                        log.info("Published messageId={} to topic={} partition={} offset={}",
                                event.messageId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
