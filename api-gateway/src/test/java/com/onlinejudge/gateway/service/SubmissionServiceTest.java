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
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
        // Default to legacy data-url mode — @Value isn't applied in unit tests,
        // so the field stays null without this. The Workstream G test below
        // overrides this to "gcs" explicitly.
        ReflectionTestUtils.setField(submissionService, "sourceMode", "data-url");
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
        assertThat(saved.getS3CodeUrl()).startsWith("data:text/plain;charset=utf-8;base64,");
        String encoded = saved.getS3CodeUrl().substring(saved.getS3CodeUrl().indexOf(',') + 1);
        assertThat(new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8))
                .isEqualTo(validRequest.getCode());
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
        assertThat(payload.get("s3CodeUrl").asText()).startsWith("data:text/plain;charset=utf-8;base64,");
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
    void accept_gcsMode_persistsGcsUriOnSubmissionAndOutbox() throws Exception {
        // Workstream G — source-mode=gcs writes to GCS BEFORE the outbox row
        // is created and the persisted s3CodeUrl is a gs:// URI (not a data: URL).
        GcsWriter gcsWriter = mock(GcsWriter.class);
        when(gcsWriter.bucket()).thenReturn("oj-source-test");
        when(gcsWriter.write(any(), eq(validRequest.getCode())))
                .thenReturn("submissions/2026/05/15/sub.txt");

        ReflectionTestUtils.setField(submissionService, "gcsWriter", gcsWriter);
        ReflectionTestUtils.setField(submissionService, "sourceMode", "gcs");

        when(submissionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        submissionService.accept(validRequest, authenticatedUserId, "us-east-1");

        verify(gcsWriter, times(1)).write(any(), eq(validRequest.getCode()));

        verify(submissionRepository).save(submissionCaptor.capture());
        verify(outboxEventRepository).save(outboxCaptor.capture());

        Submission saved = submissionCaptor.getValue();
        // Event carries the GCS URI — not a data: URL. The downstream
        // execution-worker resolves the bytes by fetching this object.
        assertThat(saved.getS3CodeUrl()).isEqualTo("gs://oj-source-test/submissions/2026/05/15/sub.txt");

        JsonNode payload = objectMapper.readTree(outboxCaptor.getValue().getPayload());
        assertThat(payload.get("s3CodeUrl").asText())
                .isEqualTo("gs://oj-source-test/submissions/2026/05/15/sub.txt");
        assertThat(payload.get("s3CodeUrl").asText()).doesNotStartWith("data:");
    }

    @Test
    void accept_gcsMode_writesToGcsBeforePersisting() throws Exception {
        // Part 6 invariant: GCS write must happen BEFORE the row hits the DB
        // (and therefore before the outbox event is observable). If the
        // mock's write() throws, no row should be saved.
        GcsWriter gcsWriter = mock(GcsWriter.class);
        when(gcsWriter.write(any(), any()))
                .thenThrow(new RuntimeException("simulated GCS outage"));

        ReflectionTestUtils.setField(submissionService, "gcsWriter", gcsWriter);
        ReflectionTestUtils.setField(submissionService, "sourceMode", "gcs");

        try {
            submissionService.accept(validRequest, authenticatedUserId, "us-east-1");
        } catch (SubmissionService.GcsWriteException expected) {
            // good
        }

        // Neither the submission nor the outbox event should have been saved.
        verify(submissionRepository, times(0)).save(any());
        verify(outboxEventRepository, times(0)).save(any());
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
