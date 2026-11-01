package com.example.agents.config;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Builds a single shared OAuth2 credential (scoped for both Gmail read/modify
 * and Sheets write) and uses it to construct the two Google API clients.
 *
 * Setup required before running:
 *   1. In Google Cloud Console, enable the Gmail API and Google Sheets API.
 *   2. Create an OAuth 2.0 Client ID (Desktop app) and download the JSON.
 *   3. Save it to the path in `agents.gmail.credentials-path` (default:
 *      src/main/resources/credentials/client_secret.json — gitignored).
 *   4. On first run, a browser window opens for consent; the resulting
 *      token is cached under `agents.gmail.tokens-directory-path` so you
 *      won't be prompted again.
 *
 * For a headless/server deployment, swap this for a Google Workspace
 * service account with domain-wide delegation instead of the installed-app
 * flow used here for local development.
 */
@Configuration
public class GoogleApiConfig {

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final List<String> SCOPES = List.of(GmailScopes.GMAIL_MODIFY, SheetsScopes.SPREADSHEETS);

    @Value("${agents.gmail.credentials-path}")
    private String credentialsPath;

    @Value("${agents.gmail.tokens-directory-path}")
    private String tokensDirectoryPath;

    @Value("${agents.google.application-name}")
    private String applicationName;

    @Bean
    public Credential googleCredential() throws IOException, GeneralSecurityException {
        var httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        GoogleClientSecrets clientSecrets;
        try (var reader = new InputStreamReader(new FileInputStream(credentialsPath))) {
            clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, reader);
        }

        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                httpTransport, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new File(tokensDirectoryPath)))
                .setAccessType("offline")
                .build();

        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("agent-user");
    }

    @Bean
    public Gmail gmailClient(Credential credential) throws GeneralSecurityException, IOException {
        return new Gmail.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, credential)
                .setApplicationName(applicationName)
                .build();
    }

    @Bean
    public Sheets sheetsClient(Credential credential) throws GeneralSecurityException, IOException {
        return new Sheets.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, credential)
                .setApplicationName(applicationName)
                .build();
    }
}
