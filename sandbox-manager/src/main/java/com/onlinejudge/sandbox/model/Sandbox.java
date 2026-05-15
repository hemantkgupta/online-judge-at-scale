package com.onlinejudge.sandbox.model;

/**
 * Mutable record of one sandbox the manager is currently tracking. Held
 * in {@code LeaseService}'s in-memory map (ephemeral — there is no
 * durable state on disk, per the blog).
 *
 * @param sandboxId        Unique per-host sandbox identifier; UUIDs.
 * @param language         Language runtime the harness rootfs is provisioned for.
 * @param firecrackerPid   PID of the {@code firecracker} process. Used to
 *                          force-kill on watchdog expiry or release.
 * @param apiSockPath      Filesystem path to the per-lease Firecracker REST socket.
 * @param vsockUdsPath     Filesystem path to the per-lease vsock UDS file
 *                          (host-to-guest connections use this + CONNECT protocol).
 * @param sessionToken     Random token issued at lease time; the
 *                          Execution Service must echo it back on
 *                          {@code /exec} and {@code /release} so a
 *                          different worker can't talk to someone else's VM.
 * @param state            Current state in the five-state lifecycle.
 * @param leasedAtEpochMs  When the lease was granted (for watchdog).
 */
public final class Sandbox {
    private final String sandboxId;
    private final String language;
    private final long firecrackerPid;
    private final String apiSockPath;
    private final String vsockUdsPath;
    private volatile String sessionToken;
    private volatile SandboxState state;
    private volatile long leasedAtEpochMs;
    private volatile String cgroupPath;
    private volatile String codeDrivePath;

    public Sandbox(String sandboxId, String language, long firecrackerPid,
                   String apiSockPath, String vsockUdsPath, String sessionToken) {
        this.sandboxId = sandboxId;
        this.language = language;
        this.firecrackerPid = firecrackerPid;
        this.apiSockPath = apiSockPath;
        this.vsockUdsPath = vsockUdsPath;
        this.sessionToken = sessionToken;
        this.state = SandboxState.PROVISIONING;
        this.leasedAtEpochMs = 0;
    }

    public String sandboxId() { return sandboxId; }
    public String language() { return language; }
    public long firecrackerPid() { return firecrackerPid; }
    public String apiSockPath() { return apiSockPath; }
    public String vsockUdsPath() { return vsockUdsPath; }
    public String sessionToken() { return sessionToken; }
    public SandboxState state() { return state; }
    public long leasedAtEpochMs() { return leasedAtEpochMs; }
    public String cgroupPath() { return cgroupPath; }
    public String codeDrivePath() { return codeDrivePath; }

    /** Mint/rotate the session token. Used when a pool-warmed sandbox
     *  transitions READY → LEASED — the token issued at provisioning
     *  time is replaced with one specific to the leasing caller. */
    public void setSessionToken(String token) { this.sessionToken = token; }

    public void setCgroupPath(String path) { this.cgroupPath = path; }
    public void setCodeDrivePath(String path) { this.codeDrivePath = path; }

    public void transition(SandboxState next) {
        this.state = next;
        if (next == SandboxState.LEASED) {
            this.leasedAtEpochMs = System.currentTimeMillis();
        }
    }
}
