package com.example.agents.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * The event published by the Gmail agent and consumed by the Sheets agent.
 * Kept as a plain record — Jackson (bundled with Spring Boot) serializes
 * records natively, no Lombok/boilerplate needed.
 */
public record GmailMessageEvent(
        String messageId,
        String threadId,
        String from,
        String subject,
        String snippet,
        Instant receivedAt,
        String correlationId
) implements Serializable {
}
