package com.onlinejudge.gateway.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.gateway.dto.SubmissionRequest;
import com.onlinejudge.gateway.dto.SubmissionResponse;
import com.onlinejudge.gateway.model.OutboxEvent;
import com.onlinejudge.gateway.model.Submission;
import com.onlinejudge.gateway.repository.OutboxEventRepository;
import com.onlinejudge.gateway.repository.SubmissionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for the Transactional Outbox pattern in SubmissionService.
 *
 * Core write-path correctness: verifies that a submission and its outbox event
 * are created atomically in a single transaction, with correct field propagation.
 */
@ExtendWith(MockitoExtension.class)
class SubmissionServiceTest {

    @Mock
    private SubmissionRepository submissionRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private SubmissionService submissionService;

    @Captor
    private ArgumentCaptor<Submission> submissionCaptor;

    @Captor
    private ArgumentCaptor<OutboxEvent> outboxCaptor;

    private SubmissionRequest validRequest;
    private String authenticatedUserId;

    @BeforeEach
    void setUp() {
        // Simulates the JWT subject the JwtAuthenticationFilter would have
        // installed into the SecurityContext — passed explicitly to accept().
        authenticatedUserId = UUID.randomUUID().toString();
        validRequest = new SubmissionRequest();
        validRequest.setProblemId(UUID.randomUUID().toString());
        validRequest.setContestId(UUID.randomUUID().toString());
        validRequest.setLanguage("python");
        validRequest.setCode("print(42)");
    }

    @Test
    void accept_persistsSubmissionAndOutboxInOneCall() throws Exception {
        // Arrange
        when(submissionRepository.save(any(Submission.class)))
                .thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any(OutboxEvent.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        // Act
        SubmissionResponse response = submissionService.accept(validRequest, authenticatedUserId, "us-east-1");

        // Assert — both saved exactly once (in the same @Transactional method)
        verify(submissionRepository, times(1)).save(submissionCaptor.capture());
        verify(outboxEventRepository, times(1)).save(outboxCaptor.capture());

        Submission savedSubmission = submissionCaptor.getValue();
        OutboxEvent savedOutbox = outboxCaptor.getValue();

        // Submission and outbox reference the same submission ID
        assertThat(savedOutbox.getSubmissionId()).isEqualTo(savedSubmission.getId());
    }

    @Test
    void accept_returnsCorrectResponse() throws Exception {
        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        SubmissionResponse response = submissionService.accept(validRequest, authenticatedUserId, "us-east-1");

        assertThat(response.getSubmissionId()).isNotNull();
        assertThat(response.getStatus()).isEqualTo("PENDING");
        assertThat(response.getGatewayTsMs()).isGreaterThan(0);
        assertThat(response.getMessage()).contains("Submission accepted");
    }

    @Test
    void accept_setsGatewayTimestampOnSubmission() throws Exception {
        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        long beforeMs = System.currentTimeMillis();
        SubmissionResponse response = submissionService.accept(validRequest, authenticatedUserId, "us-east-1");
        long afterMs = System.currentTimeMillis();

        verify(submissionRepository).save(submissionCaptor.capture());
        Submission saved = submissionCaptor.getValue();

        // T0 stamp should be between before and after the call
        assertThat(saved.getGatewayTsMs()).isBetween(beforeMs, afterMs);
        // Response should carry the same T0
        assertThat(response.getGatewayTsMs()).isEqualTo(saved.getGatewayTsMs());
    }

    @Test
    void accept_propagatesFieldsToSubmission() throws Exception {
        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        submissionService.accept(validRequest, authenticatedUserId, "us-east-1");

        verify(submissionRepository).save(submissionCaptor.capture());
        Submission saved = submissionCaptor.getValue();

        assertThat(saved.getUserId()).isEqualTo(UUID.fromString(authenticatedUserId));
        assertThat(saved.getProblemId()).isEqualTo(UUID.fromString(validRequest.getProblemId()));
        assertThat(saved.getContestId()).isEqualTo(UUID.fromString(validRequest.getContestId()));
        assertThat(saved.getLanguage()).isEqualTo("python");
        assertThat(saved.getStatus()).isEqualTo("PENDING");
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getS3CodeUrl()).startsWith("local://submissions/");
    }

    @Test
    void accept_outboxEventContainsCorrectPayload() throws Exception {
        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        submissionService.accept(validRequest, authenticatedUserId, "us-east-1");

        verify(outboxEventRepository).save(outboxCaptor.capture());
        OutboxEvent outbox = outboxCaptor.getValue();

        assertThat(outbox.getEventType()).isEqualTo("SUBMISSION_RECEIVED");
        assertThat(outbox.isPublished()).isFalse();

        // Parse the JSON payload and verify fields
        JsonNode payload = objectMapper.readTree(outbox.getPayload());
        assertThat(payload.get("userId").asText()).isEqualTo(authenticatedUserId);
        assertThat(payload.get("problemId").asText()).isEqualTo(validRequest.getProblemId());
        assertThat(payload.get("contestId").asText()).isEqualTo(validRequest.getContestId());
        assertThat(payload.get("language").asText()).isEqualTo("python");
        assertThat(payload.get("gatewayTsMs").asLong()).isGreaterThan(0);
        assertThat(payload.get("submissionId").asText()).isNotBlank();
    }

    @Test
    void accept_handlesNullContestId() throws Exception {
        validRequest.setContestId(null);

        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        SubmissionResponse response = submissionService.accept(validRequest, authenticatedUserId, "us-east-1");

        verify(submissionRepository).save(submissionCaptor.capture());
        Submission saved = submissionCaptor.getValue();

        assertThat(saved.getContestId()).isNull();
        assertThat(response.getSubmissionId()).isNotNull();
    }

    @Test
    void accept_generatesUniqueSubmissionIds() throws Exception {
        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        SubmissionResponse response1 = submissionService.accept(validRequest, authenticatedUserId, "us-east-1");
        SubmissionResponse response2 = submissionService.accept(validRequest, authenticatedUserId, "us-east-1");

        assertThat(response1.getSubmissionId()).isNotEqualTo(response2.getSubmissionId());
    }

    @Test
    void accept_stampsRegionOnSubmissionAndOutbox() throws Exception {
        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        submissionService.accept(validRequest, authenticatedUserId, "eu-west-1");

        verify(submissionRepository).save(submissionCaptor.capture());
        verify(outboxEventRepository).save(outboxCaptor.capture());

        // Submission row carries the region — REGIONAL BY ROW key in prod.
        assertThat(submissionCaptor.getValue().getRegion()).isEqualTo("eu-west-1");
        // Outbox row carries the same region so a regional changefeed reads
        // its own region's events without scanning others.
        assertThat(outboxCaptor.getValue().getRegion()).isEqualTo("eu-west-1");
    }

    @Test
    void accept_propagatesRegionIntoOutboxPayload() throws Exception {
        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        submissionService.accept(validRequest, authenticatedUserId, "ap-south-1");

        verify(outboxEventRepository).save(outboxCaptor.capture());
        JsonNode payload = objectMapper.readTree(outboxCaptor.getValue().getPayload());

        // Downstream consumers (analytics, scoring) read region off the
        // payload — must match the column.
        assertThat(payload.get("region").asText()).isEqualTo("ap-south-1");
    }
}
