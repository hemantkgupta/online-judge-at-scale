package com.onlinejudge.sandbox.api;

/**
 * Body of {@code POST /lease}. The Execution Service describes the
 * sandbox shape it needs; the Sandbox Manager picks a READY VM from the
 * pool (today: boots one on-demand) and returns the address.
 *
 * <p><b>memoryMib</b> is the per-VM memory budget — Phase 7's "per-problem
 * memory limit". For now we plumb it through but only the
 * {@code firecracker /machine-config} value uses it (no per-job cgroup
 * application yet — that's deferred).
 *
 * <p><b>cpuQuotaUs / cpuPeriodUs</b> are the per-problem CPU cap (cgroup
 * v2 cpu.max). Applied at the READY → LEASED transition by
 * {@code CgroupApplier}.
 *
 * <p><b>codeDrivePath</b> is the host filesystem path to the
 * per-submission ext4 image the worker built (Workstream B). The
 * Sandbox Manager attaches it as a second Firecracker drive at lease
 * time; the in-guest agent mounts it at {@code /code}. Nullable for
 * pool-warmup leases — see {@code PoolManager.provisionForPool}.
 *
 * <p><b>wallSeconds</b> is the host-side watchdog budget in seconds. If
 * null, the SM falls back to its configured default
 * ({@code app.firecracker.lease-wall-seconds}).
 */
public record LeaseRequest(
        String submissionId,
        String language,
        Integer memoryMib,
        Long cpuQuotaUs,
        Long cpuPeriodUs,
        String codeDrivePath,
        Integer wallSeconds
) {
    /** Back-compat 5-arg ctor for existing callers/tests. */
    public LeaseRequest(String submissionId, String language, Integer memoryMib,
                        Long cpuQuotaUs, Long cpuPeriodUs) {
        this(submissionId, language, memoryMib, cpuQuotaUs, cpuPeriodUs, null, null);
    }
}
