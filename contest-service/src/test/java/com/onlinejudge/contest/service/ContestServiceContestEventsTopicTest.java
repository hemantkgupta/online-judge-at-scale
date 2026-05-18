package com.onlinejudge.contest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.model.ContestState;
import com.onlinejudge.contest.repository.ContestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link ContestService#publishStateChange} reads the
 * contest-events topic name from configuration ({@code app.kafka.topic.contest-events})
 * rather than the legacy {@code CONTEST_EVENTS_TOPIC} hardcoded constant
 * (owner-page §11 bug).
 *
 * <p>The constant is asserted absent via reflection so we catch any future
 * re-introduction of the bug.
 *
 * <p>We use pure Mockito + {@link ReflectionTestUtils} to set the
 * {@code @Value}-injected field. A full {@code @SpringBootTest} would drag
 * in JPA / datasource / Flyway and is overkill for verifying property→field
 * binding.
 */
@ExtendWith(MockitoExtension.class)
class ContestServiceContestEventsTopicTest {

    private static final String REGIONAL_TOPIC = "contest_events.us-central1";

    @Mock private ContestRepository contestRepository;
    @Mock private EncryptionService encryptionService;
    @Mock private KafkaTemplate<String, byte[]> kafkaTemplate;

    private ContestService contestService;
    private ContestStateMachine stateMachine;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        stateMachine = new ContestStateMachine();
        objectMapper = new ObjectMapper();
        contestService = new ContestService(
                contestRepository,
                stateMachine,
                encryptionService,
                kafkaTemplate,
                objectMapper);
        // Spring would inject this from app.kafka.topic.contest-events at boot
        // — we mimic that here.
        ReflectionTestUtils.setField(contestService, "contestEventsTopic", REGIONAL_TOPIC);
    }

    @Test
    void publishStateChange_usesConfiguredRegionalTopic() {
        // Drive a state change through openRegistration: CREATED → REGISTRATION.
        UUID contestId = UUID.randomUUID();
        Contest contest = buildContest(contestId);
        when(contestRepository.findById(contestId)).thenReturn(Optional.of(contest));
        when(encryptionService.generateKey()).thenReturn("test-key");
        when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));

        contestService.openRegistration(contestId);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1))
                .send(topicCaptor.capture(), anyString(), any(byte[].class));
        assertThat(topicCaptor.getValue()).isEqualTo(REGIONAL_TOPIC);
    }

    @Test
    void publishStateChange_topicIsConfigurable_notHardcoded() {
        // Swap to a different region's topic and confirm the next publish
        // follows the field, not a constant.
        String otherTopic = "contest_events.eu-west1";
        ReflectionTestUtils.setField(contestService, "contestEventsTopic", otherTopic);

        UUID contestId = UUID.randomUUID();
        Contest contest = buildContest(contestId);
        when(contestRepository.findById(contestId)).thenReturn(Optional.of(contest));
        when(encryptionService.generateKey()).thenReturn("test-key");
        when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));

        contestService.openRegistration(contestId);

        verify(kafkaTemplate).send(eq(otherTopic), anyString(), any(byte[].class));
    }

    @Test
    void legacyHardcodedConstantIsGone() throws Exception {
        // Guard against re-introduction of the §11 bug: there must be no
        // static String constant CONTEST_EVENTS_TOPIC on ContestService.
        for (Field f : ContestService.class.getDeclaredFields()) {
            assertThat(f.getName())
                    .as("field %s should not exist — topic must come from configuration", f.getName())
                    .isNotEqualTo("CONTEST_EVENTS_TOPIC");
        }
    }

    private Contest buildContest(UUID id) {
        Contest c = new Contest();
        c.setId(id);
        c.setTitle("test");
        c.setStartTime(Instant.now().plusSeconds(3600));
        c.setEndTime(Instant.now().plusSeconds(7200));
        c.setDurationMinutes(60);
        c.setState(ContestState.CREATED);
        return c;
    }
}
