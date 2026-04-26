package com.onlinejudge.contest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.model.ContestState;
import com.onlinejudge.contest.repository.ContestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Core contest lifecycle management.
 *
 * This is the smallest service with the largest blast radius:
 * - Every API Gateway pod reads contest state for window enforcement
 * - Every Push Service instance watches for state transitions
 * - Every contestant's countdown timer depends on this service's clock
 *
 * The read-write asymmetry (millions of reads/sec, a few writes/contest)
 * drives the caching and fanout design: we write rarely, fan out aggressively,
 * and let consumers cache with short TTLs as the safety net.
 *
 * State transitions are serialized through CockroachDB's optimistic locking
 * (@Version on the Contest entity). Two concurrent transition attempts for
 * the same contest will not both succeed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ContestService {

    private final ContestRepository contestRepository;
    private final ContestStateMachine stateMachine;
    private final EncryptionService encryptionService;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String CONTEST_EVENTS_TOPIC = "contest_events";

    /**
     * Creates a new contest in CREATED state.
     */
    @Transactional
    public Contest createContest(String title, Instant startTime, Instant endTime) {
        if (startTime.isAfter(endTime)) {
            throw new IllegalArgumentException("startTime must be before endTime");
        }

        Contest contest = new Contest();
        contest.setId(UUID.randomUUID());
        contest.setTitle(title);
        contest.setStartTime(startTime);
        contest.setEndTime(endTime);
        contest.setDurationMinutes((int) Duration.between(startTime, endTime).toMinutes());
        contest.setState(ContestState.CREATED);

        contestRepository.save(contest);

        log.info("[contest] Created contest={} title='{}' start={} end={} duration={}min",
                contest.getId(), title, startTime, endTime, contest.getDurationMinutes());

        return contest;
    }

    /**
     * Opens registration: CREATED → REGISTRATION.
     *
     * Generates an AES-256 encryption key for the problem bundle.
     * In production, this is when the encrypted bundle is uploaded to CDN.
     * Locally, we store the key and a placeholder URL.
     */
    @Transactional
    public Contest openRegistration(UUID contestId) {
        Contest contest = getContestOrThrow(contestId);

        // Generate encryption key for the problem bundle
        String encryptionKey = encryptionService.generateKey();
        contest.setEncryptionKey(encryptionKey);
        contest.setEncryptedBundleUrl(
                "local://bundles/" + contestId + "/problems.encrypted");

        stateMachine.transition(contest, ContestState.REGISTRATION);

        contestRepository.save(contest);
        publishStateChange(contest, ContestState.CREATED, ContestState.REGISTRATION);

        return contest;
    }

    /**
     * Activates the contest: REGISTRATION → ACTIVE.
     *
     * This is the T0 moment. In production:
     * 1. The decryption key is published to CDN
     * 2. Clients decrypt their pre-fetched problem bundles
     * 3. API Gateway starts accepting submissions
     *
     * The key publication is the unlock event — not the countdown reaching zero.
     * A client that manipulated its clock to reach zero early would just hit
     * the key endpoint and get 404 until this transition publishes the key.
     */
    @Transactional
    public Contest activate(UUID contestId) {
        Contest contest = getContestOrThrow(contestId);

        stateMachine.transition(contest, ContestState.ACTIVE);

        contestRepository.save(contest);
        publishStateChange(contest, ContestState.REGISTRATION, ContestState.ACTIVE);

        log.info("[contest] T0: Contest {} is now ACTIVE. Decryption key published. " +
                "Submissions accepted until {}", contestId, contest.getEndTime());

        return contest;
    }

    /**
     * Closes the contest: ACTIVE → CLOSED.
     *
     * This is the T1 moment. API Gateway stops accepting new submissions.
     * In-flight executions continue. Flink drains late-arriving verdicts
     * within the watermark window (5 minutes).
     */
    @Transactional
    public Contest close(UUID contestId) {
        Contest contest = getContestOrThrow(contestId);

        stateMachine.transition(contest, ContestState.CLOSED);

        contestRepository.save(contest);
        publishStateChange(contest, ContestState.ACTIVE, ContestState.CLOSED);

        log.info("[contest] T1: Contest {} is now CLOSED. No new submissions. " +
                "Draining in-flight executions.", contestId);

        return contest;
    }

    /**
     * Publishes final results: CLOSED → RESULTS.
     *
     * Triggered after Flink confirms all events are drained and system tests
     * are complete. The leaderboard is frozen at this point.
     */
    @Transactional
    public Contest publishResults(UUID contestId) {
        Contest contest = getContestOrThrow(contestId);

        stateMachine.transition(contest, ContestState.RESULTS);

        contestRepository.save(contest);
        publishStateChange(contest, ContestState.CLOSED, ContestState.RESULTS);

        log.info("[contest] Contest {} results published. Leaderboard frozen.", contestId);

        return contest;
    }

    /**
     * Returns the contest's current state — the read path.
     * In production, this is served from an in-process cache with 1s TTL
     * in the API Gateway, not by calling this service directly.
     */
    public Optional<Contest> getContest(UUID contestId) {
        return contestRepository.findById(contestId);
    }

    /**
     * Returns the decryption key for an active contest.
     * This is the CDN endpoint that clients poll starting at T-10s.
     * Returns empty if the contest is not yet ACTIVE.
     */
    public Optional<String> getDecryptionKey(UUID contestId) {
        return contestRepository.findById(contestId)
                .filter(c -> c.getState() == ContestState.ACTIVE
                          || c.getState() == ContestState.CLOSED
                          || c.getState() == ContestState.RESULTS)
                .map(Contest::getEncryptionKey);
    }

    /**
     * Checks if a contest is currently accepting submissions.
     * Used by the API Gateway's contest window enforcement stage.
     */
    public boolean isAcceptingSubmissions(UUID contestId) {
        return contestRepository.findById(contestId)
                .map(c -> c.getState() == ContestState.ACTIVE)
                .orElse(false);
    }

    /**
     * Returns all contests that need automated lifecycle transitions.
     * Called by the LifecycleWorker on a schedule.
     */
    public List<Contest> findContestsNeedingTransition(Instant now) {
        List<Contest> pending = new ArrayList<>();

        // REGISTRATION contests whose startTime has passed → should become ACTIVE
        pending.addAll(contestRepository.findByStateAndStartTimeBefore(
                ContestState.REGISTRATION, now));

        // ACTIVE contests whose endTime has passed → should become CLOSED
        pending.addAll(contestRepository.findByStateAndEndTimeBefore(
                ContestState.ACTIVE, now));

        return pending;
    }

    private Contest getContestOrThrow(UUID contestId) {
        return contestRepository.findById(contestId)
                .orElseThrow(() -> new NoSuchElementException(
                        "Contest not found: " + contestId));
    }

    /**
     * Publishes a contest state change event to Kafka.
     *
     * Every API Gateway pod subscribes to this topic to update its local cache.
     * The Push Service subscribes to push state changes to connected clients
     * (contest started, contest closed, etc.)
     *
     * Payload: {contestId, oldState, newState, startTime, endTime, timestampMs}
     */
    private void publishStateChange(Contest contest, ContestState oldState, ContestState newState) {
        try {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("contestId", contest.getId().toString());
            event.put("oldState", oldState.name());
            event.put("newState", newState.name());
            event.put("startTime", contest.getStartTime().toEpochMilli());
            event.put("endTime", contest.getEndTime().toEpochMilli());
            event.put("timestampMs", System.currentTimeMillis());

            // Include decryption key when activating (the T0 key publication)
            if (newState == ContestState.ACTIVE) {
                event.put("decryptionKey", contest.getEncryptionKey());
            }

            byte[] payload = objectMapper.writeValueAsBytes(event);
            kafkaTemplate.send(CONTEST_EVENTS_TOPIC, contest.getId().toString(), payload);

            log.debug("[contest] Published state change: {} → {} for contest={}",
                    oldState, newState, contest.getId());

        } catch (Exception ex) {
            // State change is persisted in DB even if Kafka publish fails.
            // The 1-second TTL cache in API Gateway will pick up the change.
            log.error("[contest] Failed to publish state change for contest={}: {}",
                    contest.getId(), ex.getMessage());
        }
    }
}
