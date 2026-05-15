package com.onlinejudge.gateway.service;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link GcsWriter} — the producer half of the Part 6
 * "write-to-GCS-before-emitting-event" durability invariant.
 *
 * <p>Verifies the object-key shape (date-partitioned), the {@link BlobInfo}
 * carries the right bucket / content-type, and that a single
 * {@link Storage#create(BlobInfo, byte[], Storage.BlobTargetOption...)} call
 * is made per write. Failure-mode test asserts the writer wraps the
 * underlying storage exception in a RuntimeException so the caller can
 * decide whether to fail the request.
 */
@ExtendWith(MockitoExtension.class)
class GcsWriterTest {

    private static final String BUCKET = "oj-source-test";

    @Mock
    private Storage storage;

    @Captor
    private ArgumentCaptor<BlobInfo> blobInfoCaptor;

    @Captor
    private ArgumentCaptor<byte[]> bytesCaptor;

    private GcsWriter writer;

    @BeforeEach
    void setUp() {
        writer = new GcsWriter(storage, BUCKET);
    }

    @Test
    void write_buildsDatePartitionedKey() {
        when(storage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

        String submissionId = "11111111-2222-3333-4444-555555555555";
        String key = writer.write(submissionId, "print('hi')");

        String today = LocalDate.now(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        assertThat(key).isEqualTo("submissions/" + today + "/" + submissionId + ".txt");
    }

    @Test
    void write_callsStorageCreateExactlyOnceWithCorrectBlobInfo() {
        when(storage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

        String submissionId = "abc-def";
        String source = "int main() { return 0; }";

        writer.write(submissionId, source);

        verify(storage, times(1)).create(blobInfoCaptor.capture(), bytesCaptor.capture());

        BlobInfo blobInfo = blobInfoCaptor.getValue();
        // Bucket bound at construction-time, propagated into BlobInfo.
        assertThat(blobInfo.getBucket()).isEqualTo(BUCKET);
        // Content-type stamps every object so consumers don't have to sniff.
        assertThat(blobInfo.getContentType()).isEqualTo("text/plain; charset=utf-8");
        // Object name carries the submission ID and date partition.
        assertThat(blobInfo.getName()).contains(submissionId + ".txt");
        assertThat(blobInfo.getName()).startsWith("submissions/");

        // Bytes written verbatim, UTF-8 encoded.
        assertThat(new String(bytesCaptor.getValue(), StandardCharsets.UTF_8)).isEqualTo(source);
    }

    @Test
    void write_handlesNullSourceAsEmptyBytes() {
        when(storage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

        writer.write("sub-1", null);

        verify(storage).create(any(BlobInfo.class), bytesCaptor.capture());
        assertThat(bytesCaptor.getValue()).isEmpty();
    }

    @Test
    void write_wrapsStorageExceptionInRuntimeException() {
        when(storage.create(any(BlobInfo.class), any(byte[].class)))
                .thenThrow(new StorageException(503, "gcs unavailable"));

        assertThatThrownBy(() -> writer.write("sub-failure", "x"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("GCS write failed")
                .hasMessageContaining("sub-failure");
    }

    @Test
    void bucket_returnsConstructorArgument() {
        assertThat(writer.bucket()).isEqualTo(BUCKET);
    }
}
