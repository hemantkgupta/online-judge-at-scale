package com.onlinejudge.sandbox.api;

/**
 * Body of {@code POST /lease}'s 200 response. The Execution Service must
 * preserve {@code sandboxId} and {@code sessionToken} for the lifetime
 * of the lease — both go on the subsequent {@code /exec} call.
 *
 * <p>The blog mentions "vsock_port" in the lease response. In our shape
 * the host doesn't connect to a vsock port directly — it connects to a
 * Firecracker-provided UDS at {@code vsockUdsPath} and issues the
 * {@code CONNECT <port>} handshake to reach the guest agent. We surface
 * the port too so a future direct-AF_VSOCK client could skip the bridge.
 */
public record LeaseResponse(
        String sandboxId,
        String sessionToken,
        String vsockUdsPath,
        int vsockPort,
        String state
) {}
