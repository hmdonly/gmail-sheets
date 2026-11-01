package com.example.agents.exception;

/**
 * Thrown when the Sheets agent fails to write a row. Deliberately a
 * RuntimeException so it propagates out of the @KafkaListener method and
 * is picked up by the @RetryableTopic retry/DLT machinery.
 */
public class GoogleSheetsWriteException extends RuntimeException {
    public GoogleSheetsWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
