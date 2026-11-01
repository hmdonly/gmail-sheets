# Gmail → Kafka → Google Sheets Multi-Agent System

A two-agent system built on Spring Boot and Spring Kafka. The agents never
call each other directly — they're fully decoupled and communicate only
through Kafka, which is what lets them scale, fail, deploy, and retry
independently of one another.

```
┌─────────────────────┐        ┌───────────────────────┐        ┌──────────────────────┐
│  GmailPollingAgent   │        │        Kafka          │        │   SheetsWriterAgent   │
│  (producer)          │        │                        │        │   (consumer)          │
│                       │        │  gmail-messages        │        │                       │
│  - polls Gmail API    │──────▶│  gmail-messages-retry-0│──────▶│  - writes row to       │
│    on a schedule      │        │  gmail-messages-retry-1│        │    Google Sheets API   │
│  - builds event       │        │  gmail-messages-retry-2│        │  - manual ack on       │
│  - publishes to Kafka │        │  gmail-messages-dlt    │        │    success             │
│  - marks message read │        │                        │        │  - throws on failure   │
└─────────────────────┘        └───────────────────────┘        │    → triggers retry    │
                                                                    └──────────────────────┘
```

---

## 1. What each piece does

### Agent 1 — `GmailPollingAgent` (`gmail/GmailPollingAgent.java`)

- Runs on a fixed schedule (`@Scheduled(fixedDelayString = "${agents.gmail.poll-interval-ms}")`).
- Queries Gmail with `from:<target-sender> is:unread`.
- For each match, fetches the message metadata (`From`, `Subject`, snippet),
  builds a `GmailMessageEvent`, and publishes it via `GmailEventProducer`.
- Immediately removes the `UNREAD` label on that message via
  `gmail.users().messages().modify(...)`. This is what makes the agent
  **idempotent at the read side** — the same email won't be picked up and
  re-published on the next poll. (Kafka's idempotent producer separately
  guards against duplicate *writes* caused by network-level retries.)
- Gmail API errors during a poll are logged, not thrown — a bad poll
  doesn't crash the scheduler; the next scheduled run just tries again.

### `GmailEventProducer` (`kafka/GmailEventProducer.java`)

Thin wrapper around `KafkaTemplate`. Publishes using the Gmail message ID
as the Kafka record key, so retries/redeliveries of the same email always
land on the same partition (ordering guarantee per message).

### `GmailMessageEvent` (`model/GmailMessageEvent.java`)

The wire format between the two agents — a plain Java `record`:

```java
public record GmailMessageEvent(
    String messageId, String threadId, String from,
    String subject, String snippet, Instant receivedAt, String correlationId
) implements Serializable {}
```

Jackson (bundled with Spring Boot) serializes records natively, so no
Lombok or manual getters/builders are needed.

### Agent 2 — `SheetsWriterAgent` (`sheets/SheetsWriterAgent.java`)

- `@KafkaListener` on the main topic, with `@RetryableTopic` layered on top
  (see §2 below for what that buys you).
- On each message, calls `GoogleSheetsService.appendRow(event)`.
- On success: `ack.acknowledge()` — commits the offset (manual ack, so a
  crash mid-processing doesn't silently lose the message).
- On failure: `GoogleSheetsService` throws `GoogleSheetsWriteException`,
  which propagates out of the listener and is caught by the
  `@RetryableTopic` machinery.
- `@DltHandler` is the final landing spot for messages that failed every
  retry attempt — currently just logs with full context (message id,
  sender, exception, attempt count); this is the extension point for
  paging/alerting in a real deployment.

### `GoogleSheetsService` (`sheets/GoogleSheetsService.java`)

Appends one row per event (`messageId, from, subject, snippet, receivedAt`)
to the configured spreadsheet/range using `spreadsheets().values().append()`
with `RAW` input — no formula evaluation, values are stored exactly as sent.

---

## 2. The "enterprise" Kafka features, explained

| Feature | Implementation | Why it matters |
|---|---|---|
| **Idempotent producer** | `KafkaProducerConfig`: `acks=all`, `enable.idempotence=true`, `retries=MAX`, `max.in.flight.requests=5` | Broker de-dupes retried sends so a network blip never produces duplicate rows. `acks=all` waits for all in-sync replicas before considering the send successful. |
| **Non-blocking retry with backoff** | `@RetryableTopic` on `SheetsWriterAgent` (`attempts=4`, exponential backoff 2s→4s→8s, capped at 30s) | A failing message is republished to separate `-retry-0/1/2` topics instead of blocking the main partition — every other message keeps flowing while one is being retried. |
| **Dead Letter Topic (DLT)** | Same `@RetryableTopic` annotation, auto-creates `gmail-messages-dlt`; handled by `@DltHandler` | Messages that exhaust retries aren't dropped or left stuck — they land somewhere inspectable, with an explicit handler you control. |
| **Manual offset commit** | `KafkaConsumerConfig`: `ContainerProperties.AckMode.MANUAL_IMMEDIATE`, `enable.auto.commit=false` | At-least-once delivery — the offset only advances after the row is actually written (or explicitly handled in the DLT handler), so a crash mid-write doesn't lose the message. |
| **Deserialization-error isolation** | `ErrorHandlingDeserializer` wrapping `JsonDeserializer` | A malformed/unexpected payload fails as a normal listener exception (flows into the same retry/DLT path) instead of killing the whole listener container. |
| **Trusted-package allowlisting** | `JsonDeserializer.TRUSTED_PACKAGES` restricted to `com.example.agents.model`, `USE_TYPE_INFO_HEADERS=false` | Deserializing arbitrary classes named in message headers is a known attack vector; this pins deserialization to one explicit, known type. |
| **Explicit topic config** | `KafkaTopicConfig` declares partitions/replication via `NewTopic` bean | Avoids relying on broker auto-create defaults, which are usually wrong for production (single partition, etc.). |
| **Health checks** | Spring Boot Actuator, `management.health.kafka.enabled=true` | `/actuator/health` reports Kafka connectivity — wire this into your orchestrator's liveness/readiness probes. |

### What actually happens when a write fails

1. `SheetsWriterAgent.consume()` throws `GoogleSheetsWriteException`.
2. Attempt 1 (main topic) → fails.
3. Republished to `gmail-messages-retry-0`, waits **~2s**, retries.
4. Still failing → `gmail-messages-retry-1`, waits **~4s**, retries.
5. Still failing → `gmail-messages-retry-2`, waits **~8s**, retries.
6. Still failing after 4 total attempts → published to `gmail-messages-dlt`.
7. `SheetsWriterAgent.handleDlt()` logs it (and is where you'd add alerting).

You can watch this happen live in Kafka UI (see §5).

---

## 3. Project structure

```
gmail-sheets-kafka-agents/
├── pom.xml
├── docker-compose.yml              # local Kafka + Kafka UI
├── .gitignore
├── src/main/java/com/example/agents/
│   ├── GmailSheetsAgentsApplication.java
│   ├── config/
│   │   ├── GoogleApiConfig.java     # shared OAuth credential, Gmail + Sheets clients
│   │   ├── KafkaProducerConfig.java # idempotent producer, ObjectMapper w/ JavaTime
│   │   ├── KafkaConsumerConfig.java # ErrorHandlingDeserializer, manual ack, concurrency
│   │   └── KafkaTopicConfig.java    # main topic partitions/replication
│   ├── model/
│   │   └── GmailMessageEvent.java   # the event contract between the two agents
│   ├── gmail/
│   │   └── GmailPollingAgent.java   # Agent 1
│   ├── kafka/
│   │   └── GmailEventProducer.java  # KafkaTemplate wrapper used by Agent 1
│   ├── sheets/
│   │   ├── SheetsWriterAgent.java   # Agent 2 (+ @RetryableTopic, @DltHandler)
│   │   └── GoogleSheetsService.java # Sheets API write logic
│   └── exception/
│       └── GoogleSheetsWriteException.java
└── src/main/resources/
    ├── application.yml
    └── credentials/
        └── client_secret.example.json   # shape reference only — real file is gitignored
```

---

## 4. Prerequisites

- **Java 17+**
- **Maven 3.9+** (or use IntelliJ's bundled Maven)
- **Docker** (for local Kafka via `docker-compose.yml`) — or point at any
  Kafka cluster you already have
- A **Google Cloud project** with the Gmail API and Google Sheets API enabled
- A **Google Sheet** you own or have edit access to

---

## 5. Setup, step by step

### 5.1 Google Cloud — enable APIs and get credentials

1. Go to [console.cloud.google.com](https://console.cloud.google.com), create or select a project.
2. **APIs & Services → Library** → enable **Gmail API** and **Google Sheets API**.
3. **APIs & Services → Credentials → Create Credentials → OAuth client ID**.
   - Application type: **Desktop app**.
   - Download the resulting JSON.
4. Save it as:
   ```
   src/main/resources/credentials/client_secret.json
   ```
   (`client_secret.example.json` in the repo shows the expected shape —
   the real file is gitignored so you never commit secrets.)
5. If your OAuth consent screen is in "Testing" mode, add your own Google
   account under **Test users**, or you'll get an access-denied error
   during the consent flow.

### 5.2 Create/prepare the target Google Sheet

- Create a sheet (or use an existing one) with headers matching the columns
  written by `GoogleSheetsService` — e.g. `MessageId | From | Subject | Snippet | ReceivedAt` in row 1 of `Sheet1`.
- Copy the **spreadsheet ID** from its URL:
  `https://docs.google.com/spreadsheets/d/`**`THIS_PART`**`/edit`

### 5.3 Configure the app

Edit `src/main/resources/application.yml`:

```yaml
agents:
  gmail:
    target-sender: someone@example.com     # the sender to watch
    poll-interval-ms: 30000                # how often to poll (ms)
  sheets:
    spreadsheet-id: YOUR_SPREADSHEET_ID_HERE
    range: Sheet1!A:E
```

Leave `spring.kafka.bootstrap-servers: localhost:9092` as-is if using the
provided `docker-compose.yml`; point it at your real cluster otherwise.

### 5.4 Start Kafka locally

```bash
docker compose up -d
```

This starts Zookeeper, a single-broker Kafka, and **Kafka UI** at
[http://localhost:8081](http://localhost:8081) — useful for watching topics,
consumer lag, and (especially) the retry/DLT topics fill up.

### 5.5 Run the application

**From the command line:**
```bash
mvn spring-boot:run
```

**From IntelliJ:**
1. `File → Open` → select the `gmail-sheets-kafka-agents` folder (the one
   containing `pom.xml`). IntelliJ detects it as a Maven project and
   resolves dependencies automatically.
2. Open `GmailSheetsAgentsApplication.java` and click the green run arrow,
   or right-click → **Run**.

On first run, a browser window opens asking you to sign in and grant
consent for Gmail + Sheets access. The resulting token is cached under
`tokens/` (gitignored), so you won't be prompted again on subsequent runs.

### 5.6 Verify it's working

1. Send yourself (or have someone send) an email from the address set as
   `target-sender`.
2. Within `poll-interval-ms`, watch the application logs — you should see:
   ```
   Found 1 new message(s) from someone@example.com
   Published messageId=... to topic=gmail-messages partition=0 offset=0
   Consuming messageId=... from=someone@example.com
   Wrote messageId=... to Google Sheets
   ```
3. Check the Google Sheet — a new row should appear.
4. Check `http://localhost:8081` (Kafka UI) — you should see one message
   consumed on `gmail-messages` with no lag.
5. Check `http://localhost:8080/actuator/health` — should report `UP`,
   including a `kafka` health indicator.

### 5.7 Exercise the retry/DLT path (optional but recommended)

To see the failure path actually work:

1. Temporarily revoke the app's Sheets access, or set an invalid
   `spreadsheet-id`, or share the sheet as read-only for the OAuth user.
2. Trigger a new message from the target sender.
3. Watch the logs step through `gmail-messages` → `-retry-0` → `-retry-1`
   → `-retry-2` → `gmail-messages-dlt`, with increasing delays between
   attempts.
4. Watch the topics appear/fill up in Kafka UI in real time.
5. Restore access, revert your change, and confirm new messages flow
   through cleanly again.

---

## 6. Troubleshooting

| Symptom | Likely cause |
|---|---|
| Browser doesn't open for OAuth consent | Port `8888` (used by `LocalServerReceiver`) may be in use — free it or change the port in `GoogleApiConfig`. |
| `403` during OAuth consent | Your Google account isn't added as a **Test user** on the OAuth consent screen (if it's in Testing mode). |
| No messages picked up | Confirm the sender actually has **unread** mail matching `from:<target-sender>` in Gmail search — the agent uses the exact same query. |
| `Connection to node -1 could not be established` | Kafka isn't running / `bootstrap-servers` doesn't match — check `docker compose ps` and the port mapping. |
| Sheets write fails with `403` | The OAuth user needs edit access to the target spreadsheet; also confirm the Sheets API is enabled on the GCP project. |
| Message stuck retrying forever instead of reaching DLT | Confirm the thrown exception is (or extends) `GoogleSheetsWriteException` — `@RetryableTopic`'s `include=...` only catches listed exception types. |

---

## 7. Extending this system

- **Headless/server deployment**: swap the OAuth "installed app" flow in
  `GoogleApiConfig` for a Google Workspace **service account with
  domain-wide delegation** — no browser consent step needed.
- **DLT alerting**: wire `SheetsWriterAgent.handleDlt()` to Slack,
  PagerDuty, or an "errors" sheet instead of just logging.
- **Avoid mutating Gmail state**: track a persisted Gmail `historyId`
  instead of `is:unread`/marking-as-read, if you need to process messages
  without changing their read status.
- **Horizontal scaling**: increase `agents.kafka.topic.partitions` and run
  multiple instances of the app — Kafka consumer groups handle partition
  assignment automatically.
- **Add a third agent**: e.g. a monitoring agent that consumes the DLT
  topic and raises incidents, fully decoupled from the other two.
