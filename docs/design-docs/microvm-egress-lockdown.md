# Firecracker microVM Egress Lockdown

*Design document for roadmap item 3.1.*

## Problem

Firecracker microVMs today inherit the host's tap-device network. Submitted code can reach the open internet from inside the guest:

```python
# This works today inside a contestant submission
import urllib.request
urllib.request.urlopen("http://example.com", timeout=2)
```

A shell-escape or interpreter sandbox bypass in a contestant submission can phone home, exfiltrate test-case bytes (which the worker has fetched into the guest filesystem), or use the compute VM as a participant in attacks against external targets. The threat model for an online judge assumes the submitted code is hostile — there is no anti-cheat layer to depend on.

The current network plumbing: the sandbox-manager (SM) creates a `tap0` device on the host, attaches it as `eth0` inside the microVM, and the FC process inherits the host's default routing. The guest gets a private IP (`172.16.x.x/30` per microVM), but its packets flow through the host's main routing table and out the GCE VM's external IP.

The required posture: zero IP reachability from the guest. The only allowed control channel is vsock to the host. Test cases reach the guest via the worker's pull-and-push pattern over vsock — already implemented ([[vsock-control-channel]]). The network never needs to be live.

## Design

### Per-microVM network namespace

Each microVM gets a dedicated Linux network namespace on the host. The namespace contains only the loopback interface — no tap, no veth, nothing routable. Firecracker is configured with no `network-interfaces` entry, so the guest gets no `eth0` at all. From inside the guest, `ip link show` reports only `lo`.

Why a netns per microVM rather than a single shared "no-network" namespace: defense in depth. If a future change accidentally adds a tap to one microVM, the blast radius is one guest, not all of them. The namespace is also the right unit for iptables rules — even though there are no rules needed for the empty namespace, the model stays consistent if we later allow scoped egress for a specific language pool.

### Where this hooks in

The SM's lease handler today calls `jailer ... --new-network-namespace` but the namespace it creates inherits a tap. The change is in `sandbox-manager/src/main/java/.../lease/LeaseService.java` (the file that orchestrates lease creation):

1. Before `jailer` exec, the SM creates the namespace explicitly:
   ```bash
   ip netns add oj-mvm-<lease_id>
   ip netns exec oj-mvm-<lease_id> ip link set lo up
   ```
2. The `firecracker.json` for this microVM omits the `network-interfaces` array entirely.
3. `jailer` is invoked with `--netns /var/run/netns/oj-mvm-<lease_id>`.
4. On lease release, the SM deletes the namespace: `ip netns delete oj-mvm-<lease_id>`.

`jailer` already handles netns binding via the `--netns` flag — this is supported upstream. The change is configuration, not new code in `jailer`.

### Host firewall belt-and-suspenders

Even though the netns has no interface, a vulnerability in the kernel's netns isolation or in Firecracker's vsock device could potentially leak. Belt-and-suspenders: an explicit DROP rule at the host firewall for all traffic sourced from any FC PID's UID.

The SM already runs each FC process under a per-lease UID (`fc-<lease_id>`, a transient user created at lease time via `useradd`). Add an iptables rule on the compute VM at boot:

```bash
# In sandbox-manager's startup script or systemd unit
iptables -I OUTPUT -m owner --uid-owner-range 60000-65000 -j DROP
iptables -I OUTPUT -m owner --uid-owner-range 60000-65000 -d 169.254.169.254 -j DROP  # GCE metadata
```

The UID range matches the SM's lease-UID allocation pool. The metadata server rule is redundant if the first rule is correct but explicit for clarity: a leak that lets the guest reach `169.254.169.254` would expose the compute VM's service-account access token, the most serious possible exfiltration.

The vsock channel is unaffected because vsock doesn't traverse iptables — it's a separate AF_VSOCK socket family.

### Validation harness

A validation problem is added to the test corpus. The harness submits a deliberately-egressing program in each language and asserts the verdict is `RUNTIME_ERROR` with low time:

Python:
```python
import socket
import sys
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(0.5)
try:
    sock.connect(("1.1.1.1", 80))
    print("LEAK")
    sys.exit(1)
except (socket.error, socket.timeout):
    print("OK")
    sys.exit(0)
```

C++:
```cpp
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
int main() {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(80);
    inet_pton(AF_INET, "1.1.1.1", &a.sin_addr);
    struct timeval t{0, 500000};
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &t, sizeof(t));
    int r = connect(s, (sockaddr*)&a, sizeof(a));
    printf(r == 0 ? "LEAK\n" : "OK\n");
    return r == 0;
}
```

Java:
```java
import java.net.Socket;
public class Egress {
    public static void main(String[] args) throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new java.net.InetSocketAddress("1.1.1.1", 80), 500);
            System.out.println("LEAK");
            System.exit(1);
        } catch (Exception e) {
            System.out.println("OK");
        }
    }
}
```

Each of these MUST print `OK` and exit 0 inside the locked-down microVM. The output is `OK` because:

1. The socket creation succeeds (the syscall is not blocked; we are not using seccomp for network calls).
2. The `connect` fails immediately with `ENETUNREACH` because the netns has no route at all, not after a timeout.

`ENETUNREACH` is the success signature. A leak would surface either as `connect` returning 0 (catastrophic — the guest reached the internet) or as `ETIMEDOUT` after 500 ms (concerning — the guest had a route somewhere that absorbed the packet). Both are alert-worthy.

The harness is wired into the post-deploy smoke test (see CI/CD design doc) and runs nightly via a cron-triggered submission in each language. A `LEAK` outcome pages on-call.

### Sequence

```mermaid
sequenceDiagram
    participant W as execution-worker
    participant SM as sandbox-manager
    participant Host as host kernel
    participant J as jailer
    participant FC as firecracker
    W->>SM: POST /lease (language=python)
    SM->>Host: ip netns add oj-mvm-XYZ
    SM->>Host: ip netns exec oj-mvm-XYZ ip link set lo up
    SM->>Host: useradd fc-XYZ (uid 60123)
    SM->>J: jailer --netns /var/run/netns/oj-mvm-XYZ --uid 60123 ...
    J->>FC: exec inside netns under uid 60123
    Note over FC: guest sees only `lo`; no eth0
    FC-->>SM: vsock control channel up
    SM-->>W: leaseId, vsock cid
    W->>FC: exec submission via vsock
    FC-->>W: stdout / exit code via vsock
    W->>SM: DELETE /lease/XYZ
    SM->>Host: ip netns delete oj-mvm-XYZ
    SM->>Host: userdel fc-XYZ
```

### Edge cases

**DNS inside the guest.** Some languages' runtime initialisation paths perform a hostname lookup (Java in particular). With no resolver and no routes, `getaddrinfo` returns immediately with `EAI_NONAME` or similar — fast enough that it doesn't blow the per-test time budget. Validate during implementation that JVM cold-start in the locked-down namespace is no slower than today.

**Loopback inside the guest.** A submission that does `nc -l 127.0.0.1 8000 &` and connects to itself still works — loopback is in the namespace. This is fine; loopback cannot leak out.

**The vsock channel.** vsock is *not* network namespace-scoped at the host level. The FC process gets a vsock context ID, and the SM (running in the host's default netns) attaches to that CID directly. The netns lockdown has no impact on vsock.

**Submissions that fork.** Children inherit the parent's network namespace via `clone()` flags by default — they cannot escape. The seccomp-bpf filter applied to the FC guest's init also blocks `setns(2)` and `unshare(2)` (existing posture).

### Comparison with the current state

| Aspect                | Today                                | After                                  |
|-----------------------|--------------------------------------|----------------------------------------|
| Guest sees eth0       | Yes, in 172.16.x.x/30                | No, loopback only                      |
| `curl http://1.1.1.1` | Returns 1.1.1.1's response           | `ENETUNREACH` in <50 ms                |
| GCE metadata reachable | Yes via host route                  | No                                     |
| DNS resolution        | Works via host's resolver            | `EAI_NONAME` immediately               |
| Loopback in guest     | Works                                | Works                                  |
| vsock control         | Works                                | Works                                  |

## Implementation phases

**Phase A (1d) — netns lifecycle in SM.** Add the `ip netns add/exec/delete` calls to `LeaseService`. Make the netns name deterministic from the lease ID. Verify by inspecting `ip netns list` during a live lease.

**Phase B (1d) — strip network from firecracker config.** Remove the `network-interfaces` block from the generated `firecracker.json`. Pass `--netns` to `jailer`. Verify the guest sees only `lo`.

**Phase C (1d) — host iptables.** Add the OUTPUT DROP rule under the per-lease UID range. Persist via the SM's systemd unit (`ExecStartPre`). Verify with `iptables -L OUTPUT -v` after a lease.

**Phase D (1d) — validation harness.** Add the three language-specific egress-probe programs to the system test corpus. Wire into nightly CI.

**Phase E (1d) — runbook and alerts.** Cloud Monitoring alert on a `LEAK` outcome from the nightly probe. Runbook page for "egress probe failed" with the kernel-level diagnostic steps (`ip netns exec ip route`, `nsenter` to inspect the guest's view).

## Risks

**Increased lease latency from netns setup.** `ip netns add` takes ~5 ms; `ip netns delete` takes ~10 ms (it walks the namespace tear-down). Per-lease overhead grows by ~15 ms. Acceptable on top of the existing ~150 ms lease p50.

**iptables rule survives reboot.** The OUTPUT rule is added at SM startup via its systemd `ExecStartPre`. If the SM is restarted but iptables is flushed externally (e.g. by `gcloud` networking changes), the belt is gone. Mitigation: a periodic SM health check that re-asserts the rule and a Cloud Monitoring alert if the rule is missing.

**Lease leak.** If the SM crashes mid-lease, the namespace and UID are not cleaned up. Over time, namespaces and UIDs accumulate. Mitigation: a reconciler in the SM startup path that walks `/var/run/netns/oj-mvm-*` against the current lease set in CRDB and tears down orphans. Same for `getent passwd 'fc-*'`.

**Future requirement for scoped egress.** Some problem types (e.g. an API-integration challenge) might legitimately want scoped egress to one host. The current design has no path for that — the namespace is fully isolated. Post-launch enhancement: introduce a per-problem `egress_allowlist` field that materialises as a configured veth + iptables rules in a specific subset of microVMs.

**Kernel CVE in netns isolation.** Linux netns isolation has had a small handful of CVEs (e.g. CVE-2020-15852). Mitigation: the iptables UID-based DROP rule remains the second line of defense even if the namespace breaks. Keep the compute VM's kernel current.

## Acceptance criteria

1. A submission that runs `socket.connect(("1.1.1.1", 80))` returns from the connect call with `ENETUNREACH` in under 100 ms in all three supported languages.
2. A submission that runs `urllib.urlopen("http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token")` returns a network error in under 200 ms.
3. `ip netns list` on the host during a live lease shows the matching `oj-mvm-*` namespace.
4. `ip netns list` after the lease completes shows no orphan namespaces.
5. `iptables -L OUTPUT -v -n` on the compute VM shows the UID-range DROP rule with non-zero packet hits after the validation harness runs.
6. Killing the SM mid-lease via `docker kill` and restarting it leaves no orphan namespaces or `fc-*` users after the reconciler runs.
7. The nightly validation harness reports `OK` for each language; a failure pages on-call.
8. Per-lease p50 latency is within 20 ms of the pre-lockdown baseline.

## Related

- [[firecracker]] — the microVM
- [[firecracker-microvm-sandboxing]] — broader sandboxing context
- [[sandbox-manager]] — owns the lease lifecycle
- [[vsock-control-channel]] — the only path that survives lockdown
- [[untrusted-code-execution]] — threat model
- [[code-execution-sandbox]] — sibling design
- [[sandbox-lifecycle-state-machine]] — lease state transitions
