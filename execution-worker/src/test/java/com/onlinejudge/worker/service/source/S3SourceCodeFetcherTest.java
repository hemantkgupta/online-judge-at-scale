package com.onlinejudge.worker.service.source;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link S3SourceCodeFetcher}.
 *
 * <p>The {@link S3Client} is mocked — we exercise the URL parsing, the size
 * cap, and the error mapping. End-to-end coverage against a real S3 endpoint
 * is out of scope (would require LocalStack or an actual bucket); the
 * SDK itself is trusted to talk to S3 correctly.
 *
 * <p>Both schemes share a single class — {@code s3SchemeLabel_isPropagatedOnError}
 * and {@code r2SchemeLabel_isPropagatedOnError} cover the only behavioural
 * difference (the scheme tag in error messages).
 */
class S3SourceCodeFetcherTest {

    @Test
    void happyPath_returnsBytes() throws Exception {
        S3Client client = mock(S3Client.class);
        byte[] payload = "print('hello s3')\n".getBytes(StandardCharsets.UTF_8);
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenReturn(ResponseBytes.fromByteArray(
                        GetObjectResponse.builder().contentLength((long) payload.length).build(),
                        payload));

        S3SourceCodeFetcher fetcher = new S3SourceCodeFetcher(client, "s3");
        assertThat(new String(fetcher.fetch("s3://bucket/path/to/code.py"), StandardCharsets.UTF_8))
                .isEqualTo("print('hello s3')\n");
    }

    @Test
    void contentLengthExceedsCap_isRejectedBeforeBuffering() {
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenReturn(ResponseBytes.fromByteArray(
                        GetObjectResponse.builder()
                                .contentLength((long) SourceSizeGuard.SOURCE_MAX_BYTES + 1)
                                .build(),
                        new byte[1])); // body bytes don't matter — we reject from header.

        S3SourceCodeFetcher fetcher = new S3SourceCodeFetcher(client, "s3");
        assertThatThrownBy(() -> fetcher.fetch("s3://bucket/oversize"))
                .isInstanceOf(SourceTooLargeException.class)
                .hasMessageContaining("scheme=s3");
    }

    @Test
    void bytesExceedCapEvenIfHeaderUnderreports_isRejected() {
        S3Client client = mock(S3Client.class);
        byte[] big = new byte[SourceSizeGuard.SOURCE_MAX_BYTES + 1];
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenReturn(ResponseBytes.fromByteArray(
                        // contentLength says "small" but body is oversize —
                        // a buggy S3-API impl shouldn't be able to bypass the cap.
                        GetObjectResponse.builder().contentLength(10L).build(),
                        big));

        S3SourceCodeFetcher fetcher = new S3SourceCodeFetcher(client, "s3");
        assertThatThrownBy(() -> fetcher.fetch("s3://bucket/lying-header"))
                .isInstanceOf(SourceTooLargeException.class);
    }

    @Test
    void noSuchKey_isMappedToIOException() {
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenThrow(NoSuchKeyException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().errorMessage("missing").build())
                        .build());

        S3SourceCodeFetcher fetcher = new S3SourceCodeFetcher(client, "s3");
        assertThatThrownBy(() -> fetcher.fetch("s3://bucket/missing"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void authFailure_isMappedToIOException() {
        // S3 returns 403 for IAM auth failures. SDK surfaces this as S3Exception.
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenThrow((S3Exception) S3Exception.builder()
                        .statusCode(403)
                        .awsErrorDetails(AwsErrorDetails.builder().errorMessage("access denied").build())
                        .message("access denied")
                        .build());

        S3SourceCodeFetcher fetcher = new S3SourceCodeFetcher(client, "s3");
        assertThatThrownBy(() -> fetcher.fetch("s3://bucket/forbidden"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("status=403");
    }

    @Test
    void r2SchemeLabel_isPropagatedOnError() {
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenReturn(ResponseBytes.fromByteArray(
                        GetObjectResponse.builder()
                                .contentLength((long) SourceSizeGuard.SOURCE_MAX_BYTES + 1)
                                .build(),
                        new byte[1]));

        S3SourceCodeFetcher fetcher = new S3SourceCodeFetcher(client, "r2");
        assertThatThrownBy(() -> fetcher.fetch("r2://bucket/oversize"))
                .isInstanceOf(SourceTooLargeException.class)
                .hasMessageContaining("scheme=r2");
    }

    @Test
    void malformedUrl_isRejected() {
        S3SourceCodeFetcher fetcher = new S3SourceCodeFetcher(mock(S3Client.class), "s3");
        assertThatThrownBy(() -> fetcher.fetch("s3://bucket-only"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Malformed");
    }
}
