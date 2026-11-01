package com.example.agents.sheets;

import com.example.agents.exception.GoogleSheetsWriteException;
import com.example.agents.model.GmailMessageEvent;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class GoogleSheetsService {

    private final Sheets sheets;
    private final String spreadsheetId;
    private final String range;

    public GoogleSheetsService(Sheets sheets,
                                @Value("${agents.sheets.spreadsheet-id}") String spreadsheetId,
                                @Value("${agents.sheets.range}") String range) {
        this.sheets = sheets;
        this.spreadsheetId = spreadsheetId;
        this.range = range;
    }

    public void appendRow(GmailMessageEvent event) {
        List<Object> row = List.of(
                event.messageId(),
                event.from(),
                event.subject(),
                event.snippet() == null ? "" : event.snippet(),
                event.receivedAt().toString()
        );
        ValueRange body = new ValueRange().setValues(List.of(row));

        try {
            sheets.spreadsheets().values()
                    .append(spreadsheetId, range, body)
                    .setValueInputOption("RAW")
                    .execute();
        } catch (IOException e) {
            // Wrapped as a RuntimeException so it propagates out of the
            // @KafkaListener method and triggers @RetryableTopic's retry/DLT flow.
            throw new GoogleSheetsWriteException(
                    "Failed to append row for messageId=" + event.messageId(), e);
        }
    }
}
