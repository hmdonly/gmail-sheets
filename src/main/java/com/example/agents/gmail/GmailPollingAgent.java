package com.example.agents.gmail;

import com.example.agents.kafka.GmailEventProducer;
import com.example.agents.model.GmailMessageEvent;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.MessagePartHeader;
import com.google.api.services.gmail.model.ModifyMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * AGENT 1: reads Gmail, publishes to Kafka.
 *
 * On a fixed schedule, queries Gmail for unread messages from the
 * configured sender, converts each to a GmailMessageEvent, publishes it,
 * then removes the UNREAD label so the same message isn't re-published on
 * the next poll. That label removal is what makes this agent idempotent
 * at the source — Kafka's idempotent producer prevents duplicate writes
 * from network retries, and this prevents duplicate *reads*.
 */
@Component
public class GmailPollingAgent {

    private static final Logger log = LoggerFactory.getLogger(GmailPollingAgent.class);
    private static final String USER = "me";

    private final Gmail gmail;
    private final GmailEventProducer producer;
    private final String targetSender;

    public GmailPollingAgent(Gmail gmail,
                              GmailEventProducer producer,
                              @Value("${agents.gmail.target-sender}") String targetSender) {
        this.gmail = gmail;
        this.producer = producer;
        this.targetSender = targetSender;
    }

    @Scheduled(fixedDelayString = "${agents.gmail.poll-interval-ms}")
    public void poll() {
        String query = "from:" + targetSender + " is:unread";
        try {
            var listResponse = gmail.users().messages().list(USER).setQ(query).execute();
            List<Message> messages = listResponse.getMessages();

            if (messages == null || messages.isEmpty()) {
                log.debug("No new messages from {}", targetSender);
                return;
            }

            log.info("Found {} new message(s) from {}", messages.size(), targetSender);
            for (Message summary : messages) {
                processMessage(summary.getId());
            }
        } catch (IOException e) {
            // Transient Gmail API/network issue — log and let the next scheduled
            // run retry; we deliberately don't crash the scheduler thread.
            log.error("Gmail poll failed", e);
        }
    }

    private void processMessage(String messageId) throws IOException {
        Message full = gmail.users().messages().get(USER, messageId).setFormat("metadata")
                .setMetadataHeaders(List.of("From", "Subject"))
                .execute();

        String from = header(full, "From").orElse("unknown");
        String subject = header(full, "Subject").orElse("(no subject)");
        String snippet = full.getSnippet();

        GmailMessageEvent event = new GmailMessageEvent(
                full.getId(),
                full.getThreadId(),
                from,
                subject,
                snippet,
                Instant.now(),
                UUID.randomUUID().toString()
        );

        producer.publish(event);
        markAsRead(messageId);
    }

    private void markAsRead(String messageId) throws IOException {
        ModifyMessageRequest mods = new ModifyMessageRequest().setRemoveLabelIds(List.of("UNREAD"));
        gmail.users().messages().modify(USER, messageId, mods).execute();
    }

    private Optional<String> header(Message message, String name) {
        if (message.getPayload() == null || message.getPayload().getHeaders() == null) {
            return Optional.empty();
        }
        return message.getPayload().getHeaders().stream()
                .filter(h -> h.getName().equalsIgnoreCase(name))
                .map(MessagePartHeader::getValue)
                .findFirst();
    }
}
