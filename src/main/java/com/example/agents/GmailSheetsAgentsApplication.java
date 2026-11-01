package com.example.agents;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point. Boots two independent agents wired together only through Kafka:
 *   - GmailPollingAgent   (producer side) — reads Gmail, publishes events
 *   - SheetsWriterAgent   (consumer side) — consumes events, writes to Sheets
 *
 * They never call each other directly; Kafka is the integration point, which
 * is what lets them scale, fail, and retry independently.
 */
@SpringBootApplication
@EnableKafka
@EnableScheduling
public class GmailSheetsAgentsApplication {
    public static void main(String[] args) {
        SpringApplication.run(GmailSheetsAgentsApplication.class, args);
    }
}
