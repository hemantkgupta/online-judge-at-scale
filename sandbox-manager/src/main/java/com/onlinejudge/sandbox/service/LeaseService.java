package com.onlinejudge.sandbox.service;

import com.onlinejudge.sandbox.model.Sandbox;
import com.onlinejudge.sandbox.model.SandboxState;
import com.onlinejudge.sandbox.observability.SandboxMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Boots and manages Firecracker microVMs.
 *
 * <p><b>Phase C wired:</b> the warm pool is the primary lease source.
 * {@link #lease} pops a READY sandbox from {@link PoolManager}; if the
 * pool is empty for the requested language it throws
 * {@link PoolExhaustedException} (controller maps to 503). The
 * replenishment loop in {@code PoolManager} keeps target depth.
 *
 * <p><b>Phase D wired:</b> at the READY → LEASED transition we
 * <ol>
 *   <li>attach the per-submission code drive (if provided) — happens at
 *       provisioning time today because Firecracker drive hot-attach is
 *       not trivial; see {@link #provisionForPool} for the design call;</li>
 *   <li>apply cgroup v2 limits (memory.max / cpu.max / pids.max) via
 *       {@link CgroupApplier};</li>
 *   <li>mint a fresh session token and schedule the host-side watchdog
 *       via {@link WatchdogService}.</li>
 * </ol>
 *
 * <p>{@link #release} reverses: cancel the watchdog, transition LEASED
 * → DIRTY, SIGKILL the firecracker pid, clean up the cgroup, unlink
 * the UDS files, transition to TERMINATED.
 */
@Slf4j
@Service
public class LeaseService {

    @Value("${app.firecracker.binary}")
    private String firecrackerBinary;

    @Value("${app.firecracker.kernel-image}")
    private String kernelImage;

    @Value("${app.firecracker.rootfs-image}")
    private String rootfsImage;

    @Value("${app.firecracker.api-sock-dir}")
    private String apiSockDir;

    @Value("${app.firecracker.vsock-uds-dir}")
    private String vsockUdsDir;

    @Value("${app.firecracker.boot-args}")
    private String bootArgs;

    @Value("${app.firecracker.vcpus}")
    private int vcpus;

    @Value("${app.firecracker.default-memory-mib}")
    private int defaultMemoryMib;

    @Value("${app.firecracker.lease-wall-seconds}")
    private int defaultLeaseWallSeconds;

    /** Default cgroup CPU period in microseconds when callers don't
     *  supply one (cpu.max takes "<quota> <period>"). 100 ms is the
     *  cgroup v2 default. */
    @Value("${app.cgroup.default-cpu-period-us:100000}")
    private long defaultCpuPeriodUs;

    @Value("${app.cgroup.default-cpu-quota-us:100000}")
    private long defaultCpuQuotaUs;

    @Value("${app.agent.port}")
    private int agentPort;

    /**
     * Roadmap §3.1 — per-microVM egress lockdown. When true (the default
     * in prod), every leased sandbox gets its own Linux netns and the
     * Firecracker machine config drops its {@code network-interfaces}
     * block. When false, the legacy tap-device path is used (kept for
     * local dev on macOS and for tests that rely on a fully wired VM).
     */
    @Value("${app.sandbox.egress-lockdown.enabled:true}")
    private boolean egressLockdownEnabled;

    private final PoolManager poolManager;
    private final WatchdogService watchdog;
    private final CgroupApplier cgroupApplier;
    /** Netns + iptables lifecycle for the egress lockdown (§3.1). */
    private final NetnsApplier netnsApplier;
    /**
     * Workstream-H observability hooks. Records {@code sandbox.lease.latency_ms}
     * around the full lease invocation and inc/decs {@code sandbox.leases.active}
     * at LEASED/TERMINATED transitions. Watchdog-fire + pool-depth metrics
     * are wired by Workstream CD inside {@link WatchdogService} and
     * {@link PoolManager} respectively — not in this file.
     */
    private final SandboxMetrics metrics;

    public LeaseService(PoolManager poolManager,
                        WatchdogService watchdog,
                        CgroupApplier cgroupApplier,
                        SandboxMetrics metrics,
                        NetnsApplier netnsApplier) {
        this.poolManager = poolManager;
        this.watchdog = watchdog;
        this.cgroupApplier = cgroupApplier;
        this.metrics = metrics;
        this.netnsApplier = netnsApplier;
    }

    /** Active sandboxes keyed by id. Used by /release + /exec lookups. */
    private final ConcurrentHashMap<String, Sandbox> sandboxes = new ConcurrentHashMap<>();

    /** Monotonically-increasing CID counter. Firecracker reserves CIDs 0, 1
     *  (host), 2 (loopback); guests start at 3. We don't reuse CIDs even
     *  across lease cycles — keep it simple. */
    private final AtomicInteger nextCid = new AtomicInteger(3);

    public Sandbox lease(String submissionId, String language, Integer memoryMibOverride) {
        return lease(submissionId, language, memoryMibOverride,
                null, null, null, null);
    }

    /**
     * Full lease path. Pops a READY sandbox from the pool, applies
     * per-submission state (session token, cgroup limits, watchdog),
     * transitions to LEASED, returns.
     */
    public Sandbox lease(String submissionId, String language,
                         Integer memoryMibOverride,
                         Long cpuQuotaUs, Long cpuPeriodUs,
                         String codeDrivePath, Integer wallSecondsOverride) {
        // Workstream H: stamp at method entry; record at every return
        // (success and failure). PoolExhaustedException short-circuits
        // before we touch any per-submission state — record it anyway so
        // 503-rate is visible in the same histogram.
        long t0 = System.nanoTime();
        Sandbox sb = poolManager.acquire(language);
        if (sb == null) {
            metrics.recordLeaseLatency(System.nanoTime() - t0, language);
            log.warn("[sm] pool empty for language={} — returning 503", language);
            throw new PoolExhaustedException(language,
                    poolManager.retryAfterMsForLanguage(language));
        }

        try {
            // §3.1 — apply the egress lockdown before any per-submission
            // state mutates. Order matters: a failure here must release
            // the sandbox back to nothing (we forceKill in the catch
            // below, which also destroys the namespace). When the
            // feature flag is off we skip both create and destroy so
            // dev / CI hosts that can't run `ip netns` see the legacy
            // path untouched.
            if (egressLockdownEnabled) {
                netnsApplier.create(sb.sandboxId());
                netnsApplier.installIptablesRules(sb.sandboxId());
            }

            // Per-submission state. The pool-warmed sandbox arrives with
            // a placeholder session token; rotate it here so only this
            // caller's /exec /release can talk to the VM.
            String token = UUID.randomUUID().toString();
            sb.setSessionToken(token);
            sb.setCodeDrivePath(codeDrivePath);

            // Defence in depth: the sandbox is normally added to the
            // active map at pool-provision time, but unit tests that
            // mock pool.acquire(...) skip that path. putIfAbsent ensures
            // the release() lookup later succeeds either way without
            // changing prod behaviour (the same key was already there).
            sandboxes.putIfAbsent(sb.sandboxId(), sb);

            // Apply cgroup limits before transitioning to LEASED. If we
            // crashed between LEASED and cgroup-apply, the watchdog
            // would still fire but the VM could escape its memory cap
            // until the next pid placement.
            int memoryMib = memoryMibOverride != null ? memoryMibOverride : defaultMemoryMib;
            long mem = (long) memoryMib * 1024L * 1024L;
            long quota  = cpuQuotaUs  != null ? cpuQuotaUs  : defaultCpuQuotaUs;
            long period = cpuPeriodUs != null ? cpuPeriodUs : defaultCpuPeriodUs;
            cgroupApplier.applyAtLease(sb, mem, quota, period);

            sb.transition(SandboxState.LEASED);
            // Workstream H: count this sandbox toward sandbox.leases.active.
            // Matching decrement lives in release().
            metrics.incActiveLeases(language);

            // Watchdog last — fire callback assumes the sandbox is in
            // LEASED state.
            int wallSeconds = wallSecondsOverride != null
                    ? wallSecondsOverride : defaultLeaseWallSeconds;
            long fireAt = System.currentTimeMillis() + 1000L * wallSeconds;
            final String sbId = sb.sandboxId();
            watchdog.schedule(sbId, fireAt, () -> forceKill(sbId));

            log.info("[sm] LEASED submission={} sandbox={} language={} pid={} wall={}s",
                    submissionId, sb.sandboxId(), language, sb.firecrackerPid(), wallSeconds);
            metrics.recordLeaseLatency(System.nanoTime() - t0, language);
            return sb;

        } catch (RuntimeException ex) {
            // The sandbox we popped is now in a half-initialized state
            // — destroy it rather than returning it to the pool.
            log.error("[sm] LEASE post-pop failed submission={} sandbox={}: {}",
                    submissionId, sb.sandboxId(), ex.getMessage());
            try { forceKill(sb.sandboxId()); } catch (Exception ignored) {}
            // Defence in depth: forceKill→release destroys the namespace
            // for sandboxes still in the active map. If FC never started
            // (lease failed before the LEASED transition could populate
            // anything new), release() is a no-op for the netns — call
            // destroy explicitly so we never leak a namespace on the
            // failure path.
            if (egressLockdownEnabled) {
                try { netnsApplier.removeIptablesRules(sb.sandboxId()); } catch (Exception ignored) {}
                try { netnsApplier.destroy(sb.sandboxId()); } catch (Exception ignored) {}
            }
            metrics.recordLeaseLatency(System.nanoTime() - t0, language);
            throw ex;
        }
    }

    /**
     * Boots one Firecracker VM and parks it in READY for the pool.
     * Called by {@link PoolManager#replenish} on its boot executor.
     *
     * <p><b>Code-drive note:</b> pool-warmed VMs do not have a code
     * drive attached. Firecracker drive hot-attach is restricted (the
     * snapshot/restore path supports it but the live API does not in
     * stable releases), so we make a deliberate design call here:
     * <i>warm-pool VMs have no /dev/vdb at boot; the in-guest agent
     * therefore cannot mount /code until a future protocol extension
     * sends the code over vsock, OR the lease path discards the pool VM
     * and boots a per-submission one when a codeDrivePath is supplied.</i>
     * For now the pool is still useful for warm kernels + agent ack
     * latency wins; production will choose between the two strategies
     * once the agent change in Workstream F is in place.
     */
    public Sandbox provisionForPool(String language) {
        if (!preflight()) {
            throw new IllegalStateException(
                    "Firecracker artifacts missing — check binary/kernel/rootfs paths");
        }

        String sandboxId   = "sb-" + UUID.randomUUID();
        String apiSock     = Paths.get(apiSockDir,   "fc-" + sandboxId + ".sock").toString();
        String vsockUds    = Paths.get(vsockUdsDir,  "fc-" + sandboxId + "-vsock.sock").toString();
        int cid            = nextCid.getAndIncrement();
        String token       = UUID.randomUUID().toString(); // placeholder; rotated at lease

        Sandbox sb = null;
        Process firecracker = null;
        try {
            firecracker = new ProcessBuilder(firecrackerBinary, "--api-sock", apiSock)
                    .redirectErrorStream(true)
                    .start();
            sb = new Sandbox(sandboxId, language, firecracker.pid(), apiSock, vsockUds, token);
            sandboxes.put(sandboxId, sb);

            waitForApiSock(apiSock);
            configureFirecracker(apiSock, vsockUds, cid, defaultMemoryMib, null);
            sb.transition(SandboxState.READY);
            waitForGuestAgent(vsockUds);

            log.info("[sm] POOL-READY sandbox={} cid={} language={} pid={}",
                    sandboxId, cid, language, firecracker.pid());
            return sb;

        } catch (Exception ex) {
            log.error("[sm] POOL provision failed sandbox={}: {}",
                    sandboxId, ex.getMessage());
            if (firecracker != null && firecracker.isAlive()) firecracker.destroyForcibly();
            tryDelete(apiSock);
            tryDelete(vsockUds);
            if (sb != null) {
                sb.transition(SandboxState.TERMINATED);
                sandboxes.remove(sandboxId);
            }
            return null;
        }
    }

    public void release(String sandboxId) {
        Sandbox sb = sandboxes.remove(sandboxId);
        if (sb == null) {
            log.warn("[sm] RELEASE for unknown sandbox={}", sandboxId);
            return;
        }
        // Cancel watchdog before destroying — otherwise it could fire
        // during the kill sequence and try to operate on a half-dead VM.
        watchdog.cancel(sandboxId);
        sb.transition(SandboxState.DIRTY);
        try {
            killByPid(sb.firecrackerPid());
        } catch (Exception ex) {
            log.warn("[sm] kill firecracker pid={} failed: {}", sb.firecrackerPid(), ex.getMessage());
        }
        cgroupApplier.cleanup(sb);
        tryDelete(sb.apiSockPath());
        tryDelete(sb.vsockUdsPath());
        // §3.1 — tear down the netns. Iptables rules first (they
        // reference the sandbox id; safe to leave behind but cleaner
        // not to), then the namespace itself. Idempotent — release()
        // can race with the watchdog's forceKill path; whichever loses
        // simply sees a missing namespace and logs a benign warning.
        if (egressLockdownEnabled) {
            try { netnsApplier.removeIptablesRules(sb.sandboxId()); } catch (Exception ignored) {}
            try { netnsApplier.destroy(sb.sandboxId()); } catch (Exception ignored) {}
        }
        sb.transition(SandboxState.TERMINATED);
        // Workstream H: paired decrement for the inc at LEASED.
        // Whichever path got us here (worker /release, watchdog forceKill)
        // there was exactly one inc, so exactly one dec.
        metrics.decActiveLeases(sb.language());
        log.info("[sm] TERMINATED sandbox={}", sandboxId);
    }

    /** Watchdog fire callback. Idempotent with {@link #release}. */
    public void forceKill(String sandboxId) {
        Sandbox sb = sandboxes.get(sandboxId);
        if (sb == null) return;
        log.warn("[sm] WATCHDOG fired sandbox={} — force-killing", sandboxId);
        // release() handles cancel/kill/cleanup. We don't need a
        // separate path; the watchdog and the worker race to call
        // release, and ConcurrentHashMap.remove guarantees only one
        // wins.
        release(sandboxId);
    }

    public Sandbox find(String sandboxId) {
        return sandboxes.get(sandboxId);
    }

    // ---------- Firecracker REST orchestration ------------------------------

    private void waitForApiSock(String apiSock) throws InterruptedException, IOException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (!Files.exists(Paths.get(apiSock)) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        if (!Files.exists(Paths.get(apiSock))) {
            throw new IOException("firecracker API socket never appeared: " + apiSock);
        }
    }

    /**
     * Drives the Firecracker REST API to a running VM.
     * Order: boot-source, drives/rootfs, drives/code (optional), vsock,
     * machine-config, actions/InstanceStart.
     */
    private void configureFirecracker(String apiSock, String vsockUds, int cid, int memMib,
                                      String codeDrivePath)
            throws IOException, InterruptedException {
        putApi(apiSock, "/boot-source", String.format(
                "{\"kernel_image_path\":\"%s\",\"boot_args\":\"%s\"}", kernelImage, bootArgs));

        // Read-only rootfs — destroy-never-reuse means we never mutate it.
        putApi(apiSock, "/drives/rootfs", String.format(
                "{\"drive_id\":\"rootfs\",\"path_on_host\":\"%s\","
                        + "\"is_root_device\":true,\"is_read_only\":true}", rootfsImage));

        // Optional per-submission code drive. Pool-warm VMs skip this
        // because the worker hasn't built the image yet; in that case
        // the guest agent will simply have no /dev/vdb to mount until
        // a future protocol extension (see Workstream F).
        if (codeDrivePath != null && !codeDrivePath.isBlank()) {
            putApi(apiSock, "/drives/code", String.format(
                    "{\"drive_id\":\"code\",\"path_on_host\":\"%s\","
                            + "\"is_root_device\":false,\"is_read_only\":false}",
                    codeDrivePath));
        }

        // vsock device: guest sees CID, host gets a UDS file.
        putApi(apiSock, "/vsock", String.format(
                "{\"guest_cid\":%d,\"uds_path\":\"%s\"}", cid, vsockUds));

        putApi(apiSock, "/machine-config", String.format(
                "{\"vcpu_count\":%d,\"mem_size_mib\":%d}", vcpus, memMib));

        putApi(apiSock, "/actions", "{\"action_type\":\"InstanceStart\"}");
    }

    private void putApi(String sock, String path, String json)
            throws IOException, InterruptedException {
        List<String> cmd = List.of(
                "curl", "--unix-socket", sock, "-s", "-X", "PUT",
                "-H", "Content-Type: application/json",
                "-d", json,
                "http://localhost" + path);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        if (!p.waitFor(3, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("FC API PUT " + path + " timed out");
        }
        if (p.exitValue() != 0) {
            byte[] out = p.getInputStream().readAllBytes();
            throw new IOException("FC API PUT " + path + " failed: " + new String(out));
        }
    }

    /**
     * Probes the vsock UDS until the guest agent accepts a CONNECT.
     */
    private void waitForGuestAgent(String vsockUds) throws InterruptedException, IOException {
        // Wider deadline (30s) accommodates parallel pool boots on a constrained
        // host; single-VM boots typically take ~1s, but with N parallel FC
        // processes contending for vCPUs + KVM the in-guest agent can take many
        // seconds to start listening on vsock. Readiness is a one-time-per-VM
        // cost so widening this doesn't hurt steady-state lease latency.
        long deadline = System.currentTimeMillis() + 30_000;
        IOException lastErr = null;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(Paths.get(vsockUds))) {
                try {
                    probeConnect(vsockUds);
                    return;
                } catch (IOException e) {
                    lastErr = e;
                }
            }
            Thread.sleep(100);
        }
        throw new IOException("guest agent never accepted vsock CONNECT: "
                + (lastErr == null ? "<no error>" : lastErr.getMessage()));
    }

    private void probeConnect(String udsPath) throws IOException, InterruptedException {
        String script = String.format(
                "import socket,sys\n" +
                "s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)\n" +
                "s.settimeout(2)\n" +
                "s.connect('%s')\n" +
                "s.sendall(b'CONNECT %d\\n')\n" +
                "line=s.recv(64).decode('ascii','replace')\n" +
                "sys.exit(0 if line.startswith('OK') else 1)\n",
                udsPath, agentPort);
        Process p = new ProcessBuilder("python3", "-c", script)
                .redirectErrorStream(true).start();
        if (!p.waitFor(3, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("probe CONNECT timed out");
        }
        if (p.exitValue() != 0) {
            byte[] out = p.getInputStream().readAllBytes();
            throw new IOException("probe CONNECT failed: " + new String(out));
        }
    }

    private boolean preflight() {
        return Files.exists(Paths.get(firecrackerBinary))
                && Files.exists(Paths.get(kernelImage))
                && Files.exists(Paths.get(rootfsImage));
    }

    private static void killByPid(long pid) throws IOException, InterruptedException {
        Process p = new ProcessBuilder("kill", "-9", String.valueOf(pid))
                .redirectErrorStream(true).start();
        p.waitFor(2, TimeUnit.SECONDS);
    }

    private static void tryDelete(String pathStr) {
        try { Files.deleteIfExists(Path.of(pathStr)); } catch (IOException ignored) {}
    }
}
