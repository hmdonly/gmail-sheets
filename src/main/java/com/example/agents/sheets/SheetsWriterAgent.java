package com.example.agents.sheets;

import com.example.agents.exception.GoogleSheetsWriteException;
import com.example.agents.model.GmailMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * AGENT 2: consumes from Kafka, writes to Google Sheets.
 *
 * @RetryableTopic gives us, with zero extra infrastructure:
 *   - Non-blocking retries: a failed message is republished to
 *     gmail-messages-retry-0, -retry-1, -retry-2 (separate topics/partitions)
 *     with exponential backoff between attempts, instead of blocking the
 *     main partition while retrying (which would stall every other message
 *     behind it).
 *   - After all attempts are exhausted, the message is automatically
 *     published to gmail-messages-dlt (Dead Letter Topic) instead of being
 *     silently dropped or crashing the consumer.
 *   - include=... scopes retries to exceptions we consider transient
 *     (a Sheets API/network hiccup) rather than retrying on bugs.
 */
@Component
public class SheetsWriterAgent {

    private static final Logger log = LoggerFactory.getLogger(SheetsWriterAgent.class);

    private final GoogleSheetsService sheetsService;

    public SheetsWriterAgent(GoogleSheetsService sheetsService) {
        this.sheetsService = sheetsService;
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30000),
            autoCreateTopics = "true",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { GoogleSheetsWriteException.class },
            kafkaTemplate = "kafkaTemplate"
    )
    @KafkaListener(
            topics = "${agents.kafka.topic.gmail-messages}",
            groupId = "${agents.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(@Payload GmailMessageEvent event,
                         @Header(KafkaHeaders.RECEIVED_KEY) String key,
                         Acknowledgment ack) {
        log.info("Consuming messageId={} from={}", event.messageId(), event.from());

        sheetsService.appendRow(event); // throws GoogleSheetsWriteException on failure -> triggers retry/DLT

        ack.acknowledge();
        log.info("Wrote messageId={} to Google Sheets", event.messageId());
    }

    /**
     * Final resting place for messages that failed on every retry attempt.
     * In production you'd typically page/alert here (Slack, PagerDuty, an
     * "incident" email) rather than just logging — this is the extension
     * point for that.
     */
    @DltHandler
    public void handleDlt(@Payload GmailMessageEvent event,
                           @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
                           @Header(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS) int attempts,
                           Acknowledgment ack) {
        log.error("DEAD-LETTERED after {} attempts: messageId={} from={} reason={}",
                attempts, event.messageId(), event.from(), exceptionMessage);

        // TODO: send an alert / write to an "errors" sheet / open a ticket, etc.

        ack.acknowledge();
    }
}
