package com.onlinejudge.problem.service;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link GcsSigner}.
 *
 * <p>We don't sign against real GCS — we mock the {@link Storage} client and
 * assert the call site: bucket+key, TTL of 5 minutes, and V4-signature option.
 */
class GcsSignerTest {

    private static final String BUCKET = "oj-test-cases";

    @Test
    void sign_passesBucketAndKeyToStorage() throws Exception {
        Storage storage = mock(Storage.class);
        URL fake = new URL("https://storage.googleapis.com/oj-test-cases/foo?X-Goog-Signature=abc");
        when(storage.signUrl(any(BlobInfo.class), anyLong(), any(TimeUnit.class), any(Storage.SignUrlOption[].class)))
                .thenReturn(fake);

        GcsSigner signer = new GcsSigner(storage, BUCKET, null);

        String url = signer.sign("problems/p1/tests/1/input.txt");

        assertThat(url).isEqualTo(fake.toString());

        ArgumentCaptor<BlobInfo> blobCap = ArgumentCaptor.forClass(BlobInfo.class);
        ArgumentCaptor<Long> ttlCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<TimeUnit> unitCap = ArgumentCaptor.forClass(TimeUnit.class);
        verify(storage).signUrl(blobCap.capture(), ttlCap.capture(), unitCap.capture(), any(Storage.SignUrlOption[].class));

        assertThat(blobCap.getValue().getBucket()).isEqualTo(BUCKET);
        assertThat(blobCap.getValue().getName()).isEqualTo("problems/p1/tests/1/input.txt");
        assertThat(ttlCap.getValue()).isEqualTo(5L);
        assertThat(unitCap.getValue()).isEqualTo(TimeUnit.MINUTES);
    }

    @Test
    void sign_ttlIsExactlyFiveMinutes() {
        // The TTL is the load-bearing piece of the contract with the
        // Execution Service. Lock it in.
        assertThat(GcsSigner.TTL_MINUTES).isEqualTo(5L);
    }

    @Test
    void sign_usesV4Signature() throws Exception {
        Storage storage = mock(Storage.class);
        URL fake = new URL("https://example/v4");
        when(storage.signUrl(any(BlobInfo.class), anyLong(), any(TimeUnit.class), any(Storage.SignUrlOption[].class)))
                .thenReturn(fake);

        GcsSigner signer = new GcsSigner(storage, BUCKET, null);
        signer.sign("k");

        ArgumentCaptor<Storage.SignUrlOption[]> optsCap = ArgumentCaptor.forClass(Storage.SignUrlOption[].class);
        verify(storage).signUrl(any(BlobInfo.class), anyLong(), any(TimeUnit.class), optsCap.capture());

        Storage.SignUrlOption[] opts = optsCap.getValue();
        assertThat(opts).isNotEmpty();
        // SignUrlOption doesn't implement value-equals, so we crack open the
        // package-private `option` enum field to assert the V4 marker is in
        // the option set. This is reflection but it's stable across the 2.x
        // line — the enum constant has been named SIGNATURE_VERSION since
        // google-cloud-storage 1.x.
        boolean hasV4 = false;
        java.lang.reflect.Field optionField = Storage.SignUrlOption.class.getDeclaredField("option");
        optionField.setAccessible(true);
        for (Storage.SignUrlOption opt : opts) {
            Object enumValue = optionField.get(opt);
            if (enumValue != null && "SIGNATURE_VERSION".equals(enumValue.toString())) {
                hasV4 = true;
                break;
            }
        }
        assertThat(hasV4).as("signUrl should be invoked with withV4Signature()").isTrue();
    }
}
