package com.onlinejudge.worker.service.source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Single chokepoint for tech-spec §7.2's source-code size cap.
 *
 * <p>The api-gateway enforces {@code @Size(max=65536)} on {@code SubmissionRequest.code}
 * at ingress (returning 413 cleanly). The execution-worker re-validates the same cap
 * downstream of the wire because:
 *
 * <ul>
 *   <li>Out-of-band schemes ({@code s3://}, {@code r2://}, {@code http(s)://})
 *       bypass the gateway's validator — the worker fetches bytes the gateway
 *       never saw.</li>
 *   <li>Defence in depth: a misconfigured upstream that bypasses the @Size
 *       check should not let a 50 MB blob land in the sandbox.</li>
 * </ul>
 *
 * <p>The cap is duplicated as a constant here rather than imported from the
 * gateway's DTO because the worker module does not depend on api-gateway.
 * If §7.2 ever raises the cap, both sites must update.
 */
public final class SourceSizeGuard {

    /** Tech-spec §7.2 — keep in sync with {@code SubmissionRequest.code @Size(max=…)}. */
    public static final int SOURCE_MAX_BYTES = 65_536;

    private SourceSizeGuard() {}

    /**
     * Reject byte arrays larger than {@link #SOURCE_MAX_BYTES}. Returns the
     * input unchanged on success so call-sites can chain.
     */
    public static byte[] checkSize(byte[] bytes, String scheme) {
        if (bytes != null && bytes.length > SOURCE_MAX_BYTES) {
            throw new SourceTooLargeException(scheme, bytes.length);
        }
        return bytes;
    }

    /**
     * Reject an advertised content-length before download. -1 means
     * "unknown" — let it through; the streaming guard catches an oversize
     * body later.
     */
    public static void checkAdvertisedLength(long contentLength, String scheme) {
        if (contentLength > SOURCE_MAX_BYTES) {
            throw new SourceTooLargeException(scheme, contentLength);
        }
    }

    /**
     * Drain {@code in} up to {@link #SOURCE_MAX_BYTES}+1 bytes, throwing if
     * the cap is exceeded. UTF-8 decoded.
     */
    public static String readUpToCap(InputStream in, String scheme) throws IOException {
        byte[] buf = new byte[SOURCE_MAX_BYTES + 1];
        int total = 0;
        int read;
        while (total < buf.length && (read = in.read(buf, total, buf.length - total)) != -1) {
            total += read;
        }
        if (total > SOURCE_MAX_BYTES) {
            // Drain a touch more to differentiate "exactly cap" from "over cap".
            throw new SourceTooLargeException(scheme, (long) total);
        }
        return new String(buf, 0, total, StandardCharsets.UTF_8);
    }
}
