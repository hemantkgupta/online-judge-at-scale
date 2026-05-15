package com.onlinejudge.sandbox.service;

import com.onlinejudge.sandbox.model.Sandbox;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Applies cgroup v2 limits to the firecracker pid at the READY → LEASED
 * transition, and removes the cgroup directory at TERMINATED.
 *
 * <p>One cgroup per sandbox under {@code /sys/fs/cgroup/oj-sandbox/<id>/}.
 * The fields we write:
 *
 * <ul>
 *   <li>{@code memory.max} — hard memory cap in bytes (OOM-killer fires
 *       on overshoot, isolating the VM rather than evicting host pages).</li>
 *   <li>{@code cpu.max} — {@code "<quota> <period>"} in microseconds.
 *       Caps wall-time CPU, complementary to the in-Firecracker
 *       {@code vcpu_count} (which caps parallelism, not throughput).</li>
 *   <li>{@code pids.max} — hard pid count, 64 by default. Defeats fork
 *       bombs at the cgroup boundary so the host process table is safe.</li>
 * </ul>
 *
 * <p>The firecracker pid is then written into {@code cgroup.procs}; from
 * that moment all VMM and vCPU threads are accounted to this cgroup.
 *
 * <p><b>Dev-mode no-op:</b> on hosts without cgroup v2
 * (no {@code /sys/fs/cgroup/cgroup.controllers}) the applier logs and
 * returns. macOS/Windows dev boxes therefore see the rest of the lease
 * path work end-to-end without simulating a kernel.
 */
@Slf4j
@Component
public class CgroupApplier {

    private static final String ROOT = "/sys/fs/cgroup";
    private static final String CONTROLLERS_FILE = ROOT + "/cgroup.controllers";
    private static final String SANDBOX_PARENT = ROOT + "/oj-sandbox";

    /** Visible for tests; tests override with a per-test temp dir. */
    private final String cgroupRoot;

    public CgroupApplier() {
        this(SANDBOX_PARENT);
    }

    public CgroupApplier(String cgroupRoot) {
        this.cgroupRoot = cgroupRoot;
    }

    public boolean isAvailable() {
        return Files.exists(Paths.get(CONTROLLERS_FILE));
    }

    /**
     * Apply per-submission limits to a freshly-leased sandbox. Returns
     * the cgroup dir path on success, null on dev-mode skip.
     */
    public String applyAtLease(Sandbox sb, long memoryBytes,
                                long cpuQuotaUs, long cpuPeriodUs) {
        if (!isAvailable()) {
            log.debug("[sm] cgroup v2 not available — skipping limits for sandbox={}",
                    sb.sandboxId());
            return null;
        }
        Path dir = Paths.get(cgroupRoot, sb.sandboxId());
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("memory.max"), Long.toString(memoryBytes));
            Files.writeString(dir.resolve("cpu.max"),
                    cpuQuotaUs + " " + cpuPeriodUs);
            Files.writeString(dir.resolve("pids.max"), "64");
            // Move the firecracker pid in. Threads follow automatically
            // under cgroup v2's process-granular model.
            Files.writeString(dir.resolve("cgroup.procs"),
                    Long.toString(sb.firecrackerPid()));
            sb.setCgroupPath(dir.toString());
            log.info("[sm] cgroup applied sandbox={} mem={}B cpu={}/{}us pids=64",
                    sb.sandboxId(), memoryBytes, cpuQuotaUs, cpuPeriodUs);
            return dir.toString();
        } catch (IOException ex) {
            log.error("[sm] cgroup apply failed sandbox={}: {}",
                    sb.sandboxId(), ex.getMessage());
            // Lease still proceeds — the hypervisor-level memory cap is
            // already in place from /machine-config. Better to run with
            // weaker isolation than reject the lease entirely. The
            // alerting story for this is the SM's metric counter.
            return null;
        }
    }

    /** Best-effort rmdir at TERMINATED. The cgroup must already be empty
     *  (firecracker pid is dead), or rmdir(2) returns EBUSY. */
    public void cleanup(Sandbox sb) {
        String path = sb.cgroupPath();
        if (path == null) return;
        Path dir = Paths.get(path);
        if (!Files.exists(dir)) return;
        try {
            // rmdir is the only legal way to remove a cgroup — recursive
            // delete via Files.walk would try to unlink the interface
            // files (cgroup.procs etc.) which the kernel rejects.
            // Children: only sub-cgroups would block rmdir. We don't
            // create any.
            try (Stream<Path> s = Files.walk(dir)) {
                s.sorted(Comparator.reverseOrder())
                        .filter(p -> Files.isDirectory(p))
                        .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
            }
        } catch (IOException ex) {
            log.warn("[sm] cgroup cleanup failed sandbox={}: {}",
                    sb.sandboxId(), ex.getMessage());
        }
    }
}
