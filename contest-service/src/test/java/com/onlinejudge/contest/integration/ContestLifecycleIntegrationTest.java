package com.onlinejudge.contest.integration;

import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.model.ContestState;
import com.onlinejudge.contest.service.ContestStateMachine;
import com.onlinejudge.contest.service.EncryptionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test: full contest lifecycle with encryption.
 *
 * Tests the cross-cutting flow between ContestStateMachine and EncryptionService:
 * 1. Create a contest
 * 2. Generate encryption key + encrypt problem bundle
 * 3. Open registration (CREATED → REGISTRATION)
 * 4. Verify encrypted bundle can be decrypted with the stored key
 * 5. Activate (REGISTRATION → ACTIVE) — the T0 moment
 * 6. Close (ACTIVE → CLOSED) — the T1 moment
 * 7. Publish results (CLOSED → RESULTS)
 *
 * This is the full lifecycle that the blog's Part 4 describes.
 * The test verifies that encryption keys generated during registration
 * are correct for decryption at activation — the foundation of the
 * synchronized start mechanism.
 */
class ContestLifecycleIntegrationTest {

    private ContestStateMachine stateMachine;
    private EncryptionService encryptionService;

    @BeforeEach
    void setUp() {
        stateMachine = new ContestStateMachine();
        encryptionService = new EncryptionService();
    }

    @Test
    void fullLifecycle_createdToResults() {
        Contest contest = createTestContest();
        assertThat(contest.getState()).isEqualTo(ContestState.CREATED);

        // 1. Generate key and encrypt problem bundle
        String encryptionKey = encryptionService.generateKey();
        byte[] problemStatements = "Problem 1: Find the sum of N numbers\nInput: N followed by N integers\nOutput: Sum"
                .getBytes(StandardCharsets.UTF_8);
        byte[] encryptedBundle = encryptionService.encrypt(problemStatements, encryptionKey);

        // Store key and bundle URL on contest
        contest.setEncryptionKey(encryptionKey);
        contest.setEncryptedBundleUrl("local://bundles/" + contest.getId() + "/problems.encrypted");

        // 2. Open registration
        stateMachine.transition(contest, ContestState.REGISTRATION);
        assertThat(contest.getState()).isEqualTo(ContestState.REGISTRATION);

        // 3. Verify: the encrypted bundle can be decrypted with the stored key
        //    (This is what happens on the client at T0 when the key is published)
        byte[] decrypted = encryptionService.decrypt(encryptedBundle, contest.getEncryptionKey());
        assertThat(decrypted).isEqualTo(problemStatements);
        assertThat(new String(decrypted, StandardCharsets.UTF_8))
                .contains("Find the sum of N numbers");

        // 4. Activate — T0
        stateMachine.transition(contest, ContestState.ACTIVE);
        assertThat(contest.getState()).isEqualTo(ContestState.ACTIVE);

        // 5. Close — T1
        stateMachine.transition(contest, ContestState.CLOSED);
        assertThat(contest.getState()).isEqualTo(ContestState.CLOSED);

        // 6. Results
        stateMachine.transition(contest, ContestState.RESULTS);
        assertThat(contest.getState()).isEqualTo(ContestState.RESULTS);
        assertThat(contest.getState().isTerminal()).isTrue();
    }

    @Test
    void encryptionKeyIntegrity_wrongKeyCannotDecrypt() {
        String correctKey = encryptionService.generateKey();
        String wrongKey = encryptionService.generateKey();

        byte[] plaintext = "Secret problem: sort N integers".getBytes();
        byte[] encrypted = encryptionService.encrypt(plaintext, correctKey);

        // A contestant who somehow got a wrong key cannot read the problems
        assertThatThrownBy(() -> encryptionService.decrypt(encrypted, wrongKey))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void encryptionKeyIntegrity_tamperedBundleDetected() {
        String key = encryptionService.generateKey();
        byte[] plaintext = "Problem: find shortest path".getBytes();
        byte[] encrypted = encryptionService.encrypt(plaintext, key);

        // Tamper with the encrypted bundle (simulate corrupted CDN download)
        encrypted[20] ^= 0xFF;

        // GCM authentication tag detects the tampering
        assertThatThrownBy(() -> encryptionService.decrypt(encrypted, key))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void multipleContests_independentLifecycles() {
        Contest contest1 = createTestContest();
        Contest contest2 = createTestContest();

        // Contest 1 advances to ACTIVE
        contest1.setEncryptionKey(encryptionService.generateKey());
        contest1.setEncryptedBundleUrl("url1");
        stateMachine.transition(contest1, ContestState.REGISTRATION);
        stateMachine.transition(contest1, ContestState.ACTIVE);

        // Contest 2 stays in CREATED
        assertThat(contest1.getState()).isEqualTo(ContestState.ACTIVE);
        assertThat(contest2.getState()).isEqualTo(ContestState.CREATED);

        // Contest 2's transition doesn't affect contest 1
        contest2.setEncryptionKey(encryptionService.generateKey());
        contest2.setEncryptedBundleUrl("url2");
        stateMachine.transition(contest2, ContestState.REGISTRATION);
        assertThat(contest1.getState()).isEqualTo(ContestState.ACTIVE);
        assertThat(contest2.getState()).isEqualTo(ContestState.REGISTRATION);

        // Each contest has a unique encryption key
        assertThat(contest1.getEncryptionKey()).isNotEqualTo(contest2.getEncryptionKey());
    }

    @Test
    void lifecycle_cannotSkipStates() {
        Contest contest = createTestContest();

        // Cannot skip from CREATED directly to ACTIVE
        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.ACTIVE))
                .isInstanceOf(IllegalStateException.class);

        // Must go through REGISTRATION first
        contest.setEncryptionKey(encryptionService.generateKey());
        contest.setEncryptedBundleUrl("url");
        stateMachine.transition(contest, ContestState.REGISTRATION);
        stateMachine.transition(contest, ContestState.ACTIVE);

        assertThat(contest.getState()).isEqualTo(ContestState.ACTIVE);
    }

    @Test
    void lifecycle_cannotGoBackward() {
        Contest contest = createTestContest();
        contest.setEncryptionKey(encryptionService.generateKey());
        contest.setEncryptedBundleUrl("url");

        stateMachine.transition(contest, ContestState.REGISTRATION);
        stateMachine.transition(contest, ContestState.ACTIVE);

        // Cannot go back to REGISTRATION from ACTIVE
        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.REGISTRATION))
                .isInstanceOf(IllegalStateException.class);

        // Cannot go back to CREATED from ACTIVE
        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.CREATED))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void encryptDecrypt_largeProblemBundle() {
        String key = encryptionService.generateKey();

        // Simulate a realistic problem bundle: 4 problems, each with description + test cases
        StringBuilder bundle = new StringBuilder();
        for (int i = 1; i <= 4; i++) {
            bundle.append("=== Problem ").append(i).append(" ===\n");
            bundle.append("Title: Problem ").append(i).append("\n");
            bundle.append("Time Limit: 2000ms\n");
            bundle.append("Memory Limit: 256MB\n\n");
            bundle.append("Description: A long problem description...\n\n");
            // 10 test cases per problem
            for (int t = 1; t <= 10; t++) {
                bundle.append("--- Test Case ").append(t).append(" ---\n");
                bundle.append("Input: ").append("5\n1 2 3 4 5\n");
                bundle.append("Expected Output: ").append(15).append("\n\n");
            }
        }

        byte[] plaintext = bundle.toString().getBytes(StandardCharsets.UTF_8);
        byte[] encrypted = encryptionService.encrypt(plaintext, key);
        byte[] decrypted = encryptionService.decrypt(encrypted, key);

        assertThat(decrypted).isEqualTo(plaintext);
        assertThat(new String(decrypted, StandardCharsets.UTF_8))
                .contains("Problem 1")
                .contains("Problem 4")
                .contains("Test Case 10");
    }

    private Contest createTestContest() {
        Contest contest = new Contest();
        contest.setId(UUID.randomUUID());
        contest.setTitle("Integration Test Contest");
        contest.setStartTime(Instant.now().plus(1, ChronoUnit.HOURS));
        contest.setEndTime(Instant.now().plus(2, ChronoUnit.HOURS));
        contest.setDurationMinutes(60);
        contest.setState(ContestState.CREATED);
        return contest;
    }
}
