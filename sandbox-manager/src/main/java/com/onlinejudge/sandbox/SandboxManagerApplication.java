package com.onlinejudge.sandbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Sandbox Manager — Part 7 of the blog.
 *
 * <p>Per-host daemon that owns the Firecracker REST sockets, KVM access, and
 * the {@code /lease /exec /release} HTTP API that the execution-worker
 * (the Execution Service) calls. Trust-zone-isolated from the worker so the
 * privileged side (KVM, cgroups, killing VMM processes) does not share a
 * process with Kafka consumers and JPA writers.
 *
 * <p>What lives here (today):
 * <ul>
 *   <li>{@code LeaseService} — for now boots one Firecracker microVM per
 *       lease (no warm pool yet — Phase C). Vsock device configured at
 *       launch, agent inside the guest listens on port 1234.</li>
 *   <li>{@code AgentClient} — host-side vsock dialer; uses a tiny external
 *       Go helper ({@code oj-vsock-client}) so we don't need AF_VSOCK
 *       support in the JVM.</li>
 *   <li>{@code SandboxController} — the HTTP API: {@code POST /lease},
 *       {@code POST /exec/{id}}, {@code POST /release/{id}}.</li>
 * </ul>
 *
 * <p>What's deferred (with seams visible in the code):
 * <ul>
 *   <li>Pre-warmed pools per language (Phase C).</li>
 *   <li>Host-side wall-clock watchdog (Phase E).</li>
 *   <li>Per-submission cgroup application at lease time (today the only
 *       cgroup is the firecracker {@code mem_size_mib} + {@code vcpu_count}).</li>
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
public class SandboxManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SandboxManagerApplication.class, args);
    }
}
