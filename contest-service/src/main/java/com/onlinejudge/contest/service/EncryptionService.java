package com.onlinejudge.contest.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM encryption for problem bundles.
 *
 * During REGISTRATION, the Contest Service:
 * 1. Generates a random AES-256 key
 * 2. Encrypts the problem statements + test cases with that key
 * 3. Uploads the encrypted bundle to CDN (Cloudflare R2)
 * 4. Stores the key in CockroachDB (encrypted-at-rest by CRDB)
 *
 * At T0 (REGISTRATION→ACTIVE transition):
 * 5. Publishes the key to a CDN endpoint
 * 6. Clients that pre-fetched the encrypted bundle during REGISTRATION
 *    can now decrypt it locally
 *
 * GCM (Galois/Counter Mode) provides:
 * - Authenticated encryption: ciphertext + tag, verifiable integrity
 * - No padding oracle attacks (unlike CBC)
 * - Parallelizable encryption for large bundles
 *
 * Security properties:
 * - 256-bit key: 2^256 brute force resistance
 * - 96-bit IV (GCM standard): unique per encryption, prepended to ciphertext
 * - 128-bit authentication tag: tampering detection
 */
@Slf4j
@Service
public class EncryptionService {

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int KEY_SIZE_BITS = 256;
    private static final int IV_SIZE_BYTES = 12;   // GCM standard: 96 bits
    private static final int TAG_SIZE_BITS = 128;   // GCM authentication tag

    private final SecureRandom secureRandom = new SecureRandom();

    /**
     * Generates a new AES-256 key, returned as a Base64-encoded string.
     * One key per contest. Stored in CockroachDB, published at T0.
     */
    public String generateKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance(ALGORITHM);
            keyGen.init(KEY_SIZE_BITS, secureRandom);
            SecretKey key = keyGen.generateKey();
            return Base64.getEncoder().encodeToString(key.getEncoded());
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate AES-256 key", e);
        }
    }

    /**
     * Encrypts a plaintext byte array using AES-256-GCM.
     *
     * Output format: [12-byte IV] [ciphertext + 16-byte GCM tag]
     * The IV is prepended to the ciphertext so it can be extracted at decryption time.
     * Each encryption generates a fresh random IV — never reuse an IV with the same key.
     *
     * @param plaintext  the data to encrypt (problem statements + test cases)
     * @param base64Key  the AES-256 key as Base64
     * @return encrypted bytes: IV + ciphertext + GCM tag
     */
    public byte[] encrypt(byte[] plaintext, String base64Key) {
        try {
            byte[] keyBytes = Base64.getDecoder().decode(base64Key);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, ALGORITHM);

            // Generate a fresh random IV for each encryption
            byte[] iv = new byte[IV_SIZE_BYTES];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(TAG_SIZE_BITS, iv);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);

            byte[] ciphertext = cipher.doFinal(plaintext);

            // Prepend IV to ciphertext: [IV][ciphertext+tag]
            byte[] result = new byte[IV_SIZE_BYTES + ciphertext.length];
            System.arraycopy(iv, 0, result, 0, IV_SIZE_BYTES);
            System.arraycopy(ciphertext, 0, result, IV_SIZE_BYTES, ciphertext.length);

            return result;

        } catch (Exception e) {
            throw new RuntimeException("Encryption failed", e);
        }
    }

    /**
     * Decrypts AES-256-GCM ciphertext.
     *
     * Input format: [12-byte IV] [ciphertext + 16-byte GCM tag]
     * If the GCM tag verification fails (tampered data), throws an exception.
     *
     * @param encryptedData  IV + ciphertext + GCM tag (as produced by encrypt())
     * @param base64Key      the AES-256 key as Base64
     * @return decrypted plaintext
     */
    public byte[] decrypt(byte[] encryptedData, String base64Key) {
        try {
            if (encryptedData.length < IV_SIZE_BYTES + TAG_SIZE_BITS / 8) {
                throw new IllegalArgumentException(
                        "Encrypted data too short: must contain at least IV + GCM tag");
            }

            byte[] keyBytes = Base64.getDecoder().decode(base64Key);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, ALGORITHM);

            // Extract IV from the first 12 bytes
            byte[] iv = new byte[IV_SIZE_BYTES];
            System.arraycopy(encryptedData, 0, iv, 0, IV_SIZE_BYTES);

            // Extract ciphertext + GCM tag
            int ciphertextLength = encryptedData.length - IV_SIZE_BYTES;
            byte[] ciphertext = new byte[ciphertextLength];
            System.arraycopy(encryptedData, IV_SIZE_BYTES, ciphertext, 0, ciphertextLength);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(TAG_SIZE_BITS, iv);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);

            return cipher.doFinal(ciphertext);

        } catch (Exception e) {
            throw new RuntimeException("Decryption failed (tampered data or wrong key?)", e);
        }
    }
}
