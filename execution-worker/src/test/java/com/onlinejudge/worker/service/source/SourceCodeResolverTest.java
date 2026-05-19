package com.onlinejudge.worker.service.source;

import com.onlinejudge.worker.service.GcsClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Scheme-dispatch + size-cap tests for {@link SourceCodeResolver}.
 *
 * <p>This file also doubles as the regression baseline for the {@code data:}
 * happy path (L5 in the ask). The remote schemes have dedicated test files
 * alongside this one.
 */
class SourceCodeResolverTest {

    // -----------------------------------------------------------------
    // data: (L5 regression baseline — was supported before this change)
    // -----------------------------------------------------------------

    @Test
    void dataUrlBase64_decodes() {
        SourceCodeResolver resolver = new SourceCodeResolver(null, null, null, null);
        String source = "print(42)\n";
        String url = "data:text/plain;charset=utf-8;base64," +
                Base64.getEncoder().encodeToString(source.getBytes(StandardCharsets.UTF_8));

        assertThat(resolver.resolve(url)).isEqualTo(source);
    }

    @Test
    void dataUrlPlain_decodes() {
        SourceCodeResolver resolver = new SourceCodeResolver(null, null, null, null);
        assertThat(resolver.resolve("data:text/plain,print(1)")).isEqualTo("print(1)");
    }

    @Test
    void dataUrlOversize_rejected() {
        SourceCodeResolver resolver = new SourceCodeResolver(null, null, null, null);
        // 70 KB > 64 KB cap
        byte[] big = new byte[70 * 1024];
        java.util.Arrays.fill(big, (byte) 'a');
        String url = "data:text/plain;base64," + Base64.getEncoder().encodeToString(big);

        assertThatThrownBy(() -> resolver.resolve(url))
                .isInstanceOf(SourceTooLargeException.class)
                .hasMessageContaining("scheme=data");
    }

    // -----------------------------------------------------------------
    // gs:// — already supported; verify size-cap enforcement now goes
    // through the resolver too.
    // -----------------------------------------------------------------

    @Test
    void gcsHappyPath_returnsBytes() throws Exception {
        GcsClient gcs = mock(GcsClient.class);
        when(gcs.fetch(anyString())).thenReturn("print(2)".getBytes(StandardCharsets.UTF_8));

        SourceCodeResolver resolver = new SourceCodeResolver(gcs, null, null, null);
        assertThat(resolver.resolve("gs://bucket/key.txt")).isEqualTo("print(2)");
    }

    @Test
    void gcsOversize_rejected() throws Exception {
        GcsClient gcs = mock(GcsClient.class);
        byte[] big = new byte[SourceSizeGuard.SOURCE_MAX_BYTES + 1];
        when(gcs.fetch(anyString())).thenReturn(big);

        SourceCodeResolver resolver = new SourceCodeResolver(gcs, null, null, null);
        assertThatThrownBy(() -> resolver.resolve("gs://bucket/big.txt"))
                .isInstanceOf(SourceTooLargeException.class)
                .hasMessageContaining("scheme=gs");
    }

    @Test
    void gcsIoFailure_isWrappedAsRuntimeException() throws Exception {
        GcsClient gcs = mock(GcsClient.class);
        when(gcs.fetch(anyString())).thenThrow(new IOException("transport blew up"));

        SourceCodeResolver resolver = new SourceCodeResolver(gcs, null, null, null);
        assertThatThrownBy(() -> resolver.resolve("gs://bucket/key"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("GCS source fetch failed");
    }

    // -----------------------------------------------------------------
    // Routing / negative cases.
    // -----------------------------------------------------------------

    @Test
    void unwiredScheme_throwsUnsupportedSourceSchemeException() {
        SourceCodeResolver resolver = new SourceCodeResolver(null, null, null, null);
        assertThatThrownBy(() -> resolver.resolve("s3://b/k"))
                .isInstanceOf(UnsupportedSourceSchemeException.class)
                .hasMessageContaining("s3");
        assertThatThrownBy(() -> resolver.resolve("r2://b/k"))
                .isInstanceOf(UnsupportedSourceSchemeException.class)
                .hasMessageContaining("r2");
        assertThatThrownBy(() -> resolver.resolve("https://example.com/code"))
                .isInstanceOf(UnsupportedSourceSchemeException.class)
                .hasMessageContaining("http");
    }

    @Test
    void unknownScheme_throwsUnsupportedSourceSchemeException() {
        SourceCodeResolver resolver = new SourceCodeResolver(null, null, null, null);
        assertThatThrownBy(() -> resolver.resolve("ftp://example.com/code"))
                .isInstanceOf(UnsupportedSourceSchemeException.class)
                .hasMessageContaining("unknown scheme");
    }

    @Test
    void localScheme_rejectedExplicitly() {
        SourceCodeResolver resolver = new SourceCodeResolver(null, null, null, null);
        assertThatThrownBy(() -> resolver.resolve("local://blob"))
                .isInstanceOf(UnsupportedSourceSchemeException.class)
                .hasMessageContaining("data:");
    }

    @Test
    void blankUrl_rejected() {
        SourceCodeResolver resolver = new SourceCodeResolver(null, null, null, null);
        assertThatThrownBy(() -> resolver.resolve(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3CodeUrl");
        assertThatThrownBy(() -> resolver.resolve(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
