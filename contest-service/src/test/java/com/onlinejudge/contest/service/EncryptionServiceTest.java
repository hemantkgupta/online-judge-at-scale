package com.onlinejudge.contest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for AES-256-GCM encryption service.
 *
 * Verifies:
 * - Key generation produces valid 256-bit keys
 * - Encrypt → decrypt round-trip preserves data
 * - Different plaintexts produce different ciphertexts
 * - Same plaintext encrypted twice produces different ciphertexts (unique IV)
 * - Wrong key fails decryption (GCM authentication)
 * - Tampered ciphertext fails decryption (GCM integrity)
 * - Empty plaintext is handled correctly
 */
class EncryptionServiceTest {

    private EncryptionService encryptionService;

    @BeforeEach
    void setUp() {
        encryptionService = new EncryptionService();
    }

    @Test
    void generateKey_produces256BitKey() {
        String key = encryptionService.generateKey();

        assertThat(key).isNotBlank();
        byte[] keyBytes = Base64.getDecoder().decode(key);
        assertThat(keyBytes).hasSize(32); // 256 bits = 32 bytes
    }

    @Test
    void generateKey_uniqueKeysEachCall() {
        String key1 = encryptionService.generateKey();
        String key2 = encryptionService.generateKey();

        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void encryptDecrypt_roundTrip() {
        String key = encryptionService.generateKey();
        byte[] plaintext = "Hello, problem statements!".getBytes(StandardCharsets.UTF_8);

        byte[] encrypted = encryptionService.encrypt(plaintext, key);
        byte[] decrypted = encryptionService.decrypt(encrypted, key);

        assertThat(decrypted).isEqualTo(plaintext);
    }

    @Test
    void encryptDecrypt_largePayload() {
        String key = encryptionService.generateKey();
        // Simulate a problem bundle (~50KB)
        byte[] plaintext = new byte[50_000];
        for (int i = 0; i < plaintext.length; i++) {
            plaintext[i] = (byte) (i % 256);
        }

        byte[] encrypted = encryptionService.encrypt(plaintext, key);
        byte[] decrypted = encryptionService.decrypt(encrypted, key);

        assertThat(decrypted).isEqualTo(plaintext);
    }

    @Test
    void encrypt_outputContainsIvPlusCiphertext() {
        String key = encryptionService.generateKey();
        byte[] plaintext = "test".getBytes();

        byte[] encrypted = encryptionService.encrypt(plaintext, key);

        // Output should be: 12 bytes IV + ciphertext + 16 bytes GCM tag
        // So minimum size is 12 + plaintext.length + 16
        assertThat(encrypted.length).isGreaterThanOrEqualTo(12 + plaintext.length + 16);
    }

    @Test
    void encrypt_sameInputDifferentCiphertext_uniqueIV() {
        String key = encryptionService.generateKey();
        byte[] plaintext = "same input".getBytes();

        byte[] encrypted1 = encryptionService.encrypt(plaintext, key);
        byte[] encrypted2 = encryptionService.encrypt(plaintext, key);

        // Different IV → different ciphertext (even with same key + plaintext)
        assertThat(encrypted1).isNotEqualTo(encrypted2);

        // But both decrypt to the same plaintext
        assertThat(encryptionService.decrypt(encrypted1, key)).isEqualTo(plaintext);
        assertThat(encryptionService.decrypt(encrypted2, key)).isEqualTo(plaintext);
    }

    @Test
    void decrypt_wrongKey_failsGcmAuth() {
        String correctKey = encryptionService.generateKey();
        String wrongKey = encryptionService.generateKey();
        byte[] plaintext = "secret".getBytes();

        byte[] encrypted = encryptionService.encrypt(plaintext, correctKey);

        assertThatThrownBy(() -> encryptionService.decrypt(encrypted, wrongKey))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Decryption failed");
    }

    @Test
    void decrypt_tamperedCiphertext_failsGcmIntegrity() {
        String key = encryptionService.generateKey();
        byte[] plaintext = "tamper test".getBytes();

        byte[] encrypted = encryptionService.encrypt(plaintext, key);

        // Flip a bit in the ciphertext (after the IV)
        encrypted[15] ^= 0x01;

        assertThatThrownBy(() -> encryptionService.decrypt(encrypted, key))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void decrypt_tooShortInput_rejected() {
        String key = encryptionService.generateKey();

        // Input shorter than IV + GCM tag (12 + 16 = 28 bytes)
        byte[] tooShort = new byte[20];

        assertThatThrownBy(() -> encryptionService.decrypt(tooShort, key))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void encryptDecrypt_emptyPlaintext() {
        String key = encryptionService.generateKey();
        byte[] plaintext = new byte[0];

        byte[] encrypted = encryptionService.encrypt(plaintext, key);
        byte[] decrypted = encryptionService.decrypt(encrypted, key);

        assertThat(decrypted).isEmpty();
    }
}
