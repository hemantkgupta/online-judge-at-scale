package com.onlinejudge.sandbox.firecracker;

import com.onlinejudge.sandbox.service.NetnsApplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds the argv used to start a Firecracker process and the JSON
 * fragments PUT against its REST API. Today most of the orchestration
 * still lives in {@code LeaseService} (history reasons), so this class
 * is intentionally narrow: it owns the two pieces of configuration that
 * the egress-lockdown work (§3.1) touches.
 *
 * <ol>
 *   <li>{@link #buildFirecrackerArgv} — prepends {@code ip netns exec
 *       oj-&lt;id&gt;} so the FC process inherits a namespace with zero
 *       network interfaces. When the lockdown is disabled (dev / CI on
 *       macOS) the prefix is empty and the legacy command is returned
 *       unchanged.</li>
 *   <li>{@link #machineConfigJson} — emits the {@code /machine-config}
 *       body for Firecracker. Historically this also carried a
 *       {@code network-interfaces} array pointing at a host tap device;
 *       <b>that field is now omitted entirely</b>. With no
 *       {@code network-interfaces} entry FC creates zero virtio-net
 *       devices and the guest boots with only {@code lo}. The vsock
 *       device is configured via a separate {@code /vsock} PUT and is
 *       unaffected.</li>
 * </ol>
 *
 * <h2>What got removed</h2>
 * The pre-lockdown JSON had a block of the form:
 * <pre>{@code
 *   "network-interfaces": [
 *     { "iface_id": "eth0",
 *       "host_dev_name": "fc-tap-<id>",
 *       "guest_mac": "AA:FC:00:00:00:01" }
 *   ]
 * }</pre>
 * Removing it is the difference between "guest has an eth0 reachable
 * over 172.16/12" and "guest only sees lo". See the design doc at
 * {@code raw-blog/oj-design-docs/microvm-egress-lockdown.md}.
 */
@Slf4j
@Component
public class FirecrackerLauncher {

    @Value("${app.firecracker.binary}")
    private String firecrackerBinary;

    @Value("${app.sandbox.egress-lockdown.enabled:true}")
    private boolean egressLockdownEnabled;

    private final NetnsApplier netnsApplier;

    public FirecrackerLauncher(NetnsApplier netnsApplier) {
        this.netnsApplier = netnsApplier;
    }

    /**
     * Build the argv to start one Firecracker process. When the egress
     * lockdown is enabled, prefixes the command with {@code ip netns
     * exec oj-&lt;id&gt;} so the FC binary opens its devices inside the
     * pre-created namespace. When disabled, returns the plain
     * {@code firecracker --api-sock} command.
     */
    public List<String> buildFirecrackerArgv(String sandboxId, String apiSock) {
        List<String> argv = new ArrayList<>();
        if (egressLockdownEnabled) {
            argv.addAll(netnsApplier.execPrefix(sandboxId));
        }
        argv.add(firecrackerBinary);
        argv.add("--api-sock");
        argv.add(apiSock);
        return Collections.unmodifiableList(argv);
    }

    /**
     * Emit the JSON body for {@code PUT /machine-config}. Crucially this
     * does <i>not</i> include a {@code network-interfaces} array — the
     * one-line summary of §3.1.
     */
    public String machineConfigJson(int vcpus, int memoryMib) {
        return String.format(
                "{\"vcpu_count\":%d,\"mem_size_mib\":%d}", vcpus, memoryMib);
    }
}
