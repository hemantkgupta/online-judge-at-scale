# ADR-0004: Firecracker over Docker in Production

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

The OJ runs untrusted, adversarial code from anonymous contestants at up to 14,000 submissions per second peak. Sandboxing is not a feature — it is the only thing standing between contestant code and either (a) the host kernel and other contestants' data or (b) the public internet. The Judge0 CVE history is the cautionary tale: CVE-2024-28185 (CVSS 10.0, container escape via symlink race) and CVE-2024-29021 (CVSS 9.1, SSRF through the sandbox). Both exploit the shared-kernel architecture of standard Docker containers.

The forcing function: choose an isolation boundary whose security model is appropriate to the threat (anonymous adversaries) and whose performance fits the latency budget (sub-2-second pretest verdict).

## Decision

Use Firecracker microVMs as the production sandbox. Each submission gets its own VM with its own guest kernel. The contestant's code runs inside that kernel; an attacker would have to escape the hypervisor (KVM) — not just the kernel — to reach the host. Boot time ~125 ms; memory overhead ~5-10 MB per VM.

Sandbox-manager owns the pool: warm VMs are pre-booted (see [`flows/pool-replenishment.md`](../flows/pool-replenishment.md)) so `/lease` is constant-time. Every VM is destroyed after release (never recycled — the security boundary > the boot cost).

## Alternatives considered

**Docker containers (with cgroups + namespaces + seccomp).** The shared host kernel is the same attack surface as Judge0. Docker's defense-in-depth (read-only fs, dropped caps, seccomp profile, no networking) is what the dev backend uses on macOS — accepted as a weaker boundary for dev only. Production would expose the OJ to the entire history of container-escape CVEs.

**gVisor (`runsc`).** Userspace kernel that intercepts syscalls. Stronger than `runc`, weaker than a hardware-isolated VM. Available in the OJ as `app.sandbox.docker.runtime=runsc`. Some workloads (heavy compute, specific syscalls) don't run on it. Rejected as production primary for the same reason: a kernel exploit becomes a gVisor exploit if you can find one.

**Direct KVM without Firecracker.** KVM is the underlying mechanism Firecracker uses. Calling it directly means reinventing the device model, the API surface, and the recovery story. Firecracker is the minimal API + minimal device set version of exactly this, designed by AWS Lambda for the same threat model.

**Per-host VM with chroots inside.** A host VM that fork-execs into chroots for each submission. Some isolation, no kernel-level boundary. Same shared-kernel problem.

## Consequences

**Positive:**
- Hardware-isolated boundary. A contestant's syscall hits a per-submission guest kernel, not the host.
- Resource caps are enforced by the hypervisor, not by the kernel that the attacker may have compromised.
- Per-VM network namespace + no interfaces + vsock-only egress = a cleaner network-isolation model than Docker's. See [`design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md).
- Track record: AWS Lambda + Fly.io run untrusted workloads on Firecracker without a documented escape CVE.
- Boot time amortised: the warm pool absorbs the 125 ms cost in the background.

**Negative:**
- Requires Linux + `/dev/kvm`. macOS users get the Docker dev backend (documented gap).
- Memory overhead: 21,000 concurrent VMs × 10 MB = 210 GB. Spread across the compute fleet, but real.
- Operational complexity: KVM, network namespaces, vsock, jailer. The sandbox-manager owner page is the longest in the repo (~4.2K words) for a reason.
- The vsock-only channel between worker and agent requires a custom bridge — see [ADR-0006](./0006-vsock-go-bridge-not-jni.md).

## Implementation pointers

- Production execution backend: `execution-worker/.../service/FirecrackerExecutionService.java`.
- Dev fallback: `execution-worker/.../service/DockerExecutionService.java`.
- Firecracker spawn: `sandbox-manager/.../firecracker/FirecrackerLauncher.java`.
- Per-VM netns: `sandbox-manager/.../network/NetworkNamespaceManager.java` (see also the lockdown design doc).
- In-guest agent runs as PID 1: `infra/firecracker/agent/cmd/agent/main.go`.

## Related

- [`tech-spec.md#6-sandbox-architecture-deep-dive`](../tech-spec.md#6-sandbox-architecture-deep-dive)
- [`services/sandbox-manager.md`](../services/sandbox-manager.md)
- [`design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md)
- [Firecracker NSDI paper](https://www.usenix.org/conference/nsdi20/presentation/agache)
- [Judge0 CVE-2024-28185](https://nvd.nist.gov/vuln/detail/CVE-2024-28185)
