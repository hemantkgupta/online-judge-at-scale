package com.onlinejudge.sandbox.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Manages the per-sandbox Linux network namespace that locks egress
 * down to "vsock only" — roadmap item 3.1.
 *
 * <p>Each leased microVM gets its own netns containing only the loopback
 * interface. Firecracker is started inside that netns (via {@link
 * #execPrefix}) with no {@code network-interfaces} in its machine
 * config, so the guest boots with zero virtio-net devices. The only
 * surviving host↔guest channel is vsock, which is an AF_VSOCK socket
 * family and bypasses netns entirely.
 *
 * <p>Defence in depth: optional iptables rules on the host's default
 * netns drop any packet that somehow originates from a tap device named
 * {@code fc-tap-<id>} or from the firecracker UID. The iptables tier is
 * gated on {@code app.sandbox.egress-lockdown.iptables-enabled} (off
 * by default outside Linux/prod) so unit tests and macOS dev loops do
 * not need privileged firewall access.
 *
 * <h2>Why shell out to {@code ip netns}</h2>
 * The Firecracker docs themselves recommend invoking the {@code ip}
 * tool — no need for a JNI/netlink binding. The userspace tool is
 * always present on the compute VMs (it lives in {@code iproute2}, a
 * base package) and the operations are infrequent (once per lease).
 *
 * <h2>Naming</h2>
 * Linux netns names are capped at 15 characters. {@link #netnsName}
 * trims accordingly. We prefix with {@code oj-} so a host operator can
 * eyeball {@code ip netns list} and spot which namespaces belong to the
 * judge.
 */
public interface NetnsApplier {

    /** Create the netns and bring up its loopback interface. Idempotent
     *  by default (a pre-existing netns with the same name is logged
     *  and reused — this keeps cleanup races benign). */
    void create(String sandboxId);

    /** Destroy the netns. Idempotent — {@code ip netns del} on a missing
     *  namespace returns non-zero on some distros and zero on others;
     *  in either case we swallow and log. */
    void destroy(String sandboxId);

    /** Compute the netns name from a sandbox id. Capped at 15 chars. */
    String netnsName(String sandboxId);

    /** Return the argv prefix that runs a child process inside the netns:
     *  {@code ["ip", "netns", "exec", netnsName(sandboxId)]}. The
     *  caller appends the binary + args (typically the firecracker
     *  command). */
    List<String> execPrefix(String sandboxId);

    /** Install the belt-and-suspenders iptables DROP rules for one
     *  sandbox. A no-op when {@code iptables-enabled=false} (the
     *  default off-Linux). */
    void installIptablesRules(String sandboxId);

    /** Remove the iptables rules installed by {@link #installIptablesRules}.
     *  Idempotent. */
    void removeIptablesRules(String sandboxId);

    /**
     * Default impl backed by {@code /usr/sbin/ip} and {@code /usr/sbin/iptables}.
     * Tests swap in an in-memory {@link Function} that captures argv and
     * returns a mock {@link Process}.
     */
    @Slf4j
    @Component
    class DefaultNetnsApplier implements NetnsApplier {

        /** Linux netns name length cap. */
        static final int MAX_NETNS_NAME = 15;

        @Value("${app.sandbox.egress-lockdown.iptables-enabled:false}")
        private boolean iptablesEnabled = false;

        // Field-initialised defaults so unit tests that bypass Spring's
        // @Value processing (constructing a {@code new
        // DefaultNetnsApplier()} directly) still get a sane binary
        // path. The @Value annotation will overwrite these when Spring
        // wires the bean.
        @Value("${app.sandbox.egress-lockdown.ip-binary:/usr/sbin/ip}")
        private String ipBinary = "/usr/sbin/ip";

        @Value("${app.sandbox.egress-lockdown.iptables-binary:/usr/sbin/iptables}")
        private String iptablesBinary = "/usr/sbin/iptables";

        /** Process factory seam. Default invokes {@link ProcessBuilder};
         *  tests override to capture argv without forking. Package-private
         *  setter rather than a constructor arg so Spring can wire the
         *  bean without test-only ctor noise. */
        private Function<List<String>, Process> processFactory = argv -> {
            try {
                return new ProcessBuilder(argv).redirectErrorStream(true).start();
            } catch (IOException e) {
                throw new RuntimeException(
                        "failed to spawn " + argv.get(0), e);
            }
        };

        /** Visible for tests. */
        void setProcessFactory(Function<List<String>, Process> factory) {
            this.processFactory = factory;
        }

        /** Visible for tests. */
        void setIptablesEnabled(boolean enabled) {
            this.iptablesEnabled = enabled;
        }

        @Override
        public String netnsName(String sandboxId) {
            String raw = "oj-" + sandboxId;
            if (raw.length() <= MAX_NETNS_NAME) return raw;
            return raw.substring(0, MAX_NETNS_NAME);
        }

        @Override
        public List<String> execPrefix(String sandboxId) {
            return List.of(ipBinary, "netns", "exec", netnsName(sandboxId));
        }

        @Override
        public void create(String sandboxId) {
            String name = netnsName(sandboxId);
            // ip netns add <name>
            int rc = run(List.of(ipBinary, "netns", "add", name));
            if (rc != 0) {
                // Could be "File exists" on a stale namespace — try to
                // delete + recreate so we don't fail an entire lease for
                // a leftover from a crashed sibling.
                log.warn("[netns] add {} returned rc={} — attempting delete+retry",
                        name, rc);
                run(List.of(ipBinary, "netns", "del", name));
                rc = run(List.of(ipBinary, "netns", "add", name));
                if (rc != 0) {
                    throw new IllegalStateException(
                            "ip netns add " + name + " failed rc=" + rc);
                }
            }
            // Bring loopback up inside the namespace — without this, the
            // guest's `lo` is DOWN and even 127.0.0.1 connect()s fail.
            int loRc = run(List.of(ipBinary, "netns", "exec", name,
                    ipBinary, "link", "set", "lo", "up"));
            if (loRc != 0) {
                log.warn("[netns] failed to bring lo up in {} rc={}", name, loRc);
            }
            log.info("[netns] created {} for sandbox={}", name, sandboxId);
        }

        @Override
        public void destroy(String sandboxId) {
            String name = netnsName(sandboxId);
            int rc = run(List.of(ipBinary, "netns", "del", name));
            if (rc != 0) {
                // Not fatal — netns may already be gone from a crash.
                log.warn("[netns] del {} returned rc={} (assuming already gone)",
                        name, rc);
            } else {
                log.info("[netns] destroyed {} for sandbox={}", name, sandboxId);
            }
        }

        @Override
        public void installIptablesRules(String sandboxId) {
            if (!iptablesEnabled) return;
            for (List<String> rule : iptablesRules(sandboxId, "-I")) {
                int rc = run(rule);
                if (rc != 0) {
                    log.warn("[netns] iptables install rc={} argv={}", rc, rule);
                }
            }
        }

        @Override
        public void removeIptablesRules(String sandboxId) {
            if (!iptablesEnabled) return;
            for (List<String> rule : iptablesRules(sandboxId, "-D")) {
                int rc = run(rule);
                if (rc != 0) {
                    log.warn("[netns] iptables remove rc={} argv={}", rc, rule);
                }
            }
        }

        /**
         * The two belt-and-suspenders rules. Built with the requested
         * {@code op} so install ({@code -I}, insert-at-top) and remove
         * ({@code -D}) share one definition and can never drift.
         */
        private List<List<String>> iptablesRules(String sandboxId, String op) {
            String tap = "fc-tap-" + sandboxId;
            List<List<String>> rules = new ArrayList<>();
            // Drop everything forwarded out of this sandbox's tap.
            rules.add(Arrays.asList(iptablesBinary, op, "FORWARD",
                    "-i", tap, "-j", "DROP"));
            // Refuse the firecracker UID from binding any off-host
            // destination. We use REJECT (icmp-port-unreachable) so the
            // process fails fast rather than hanging — matches the
            // ENETUNREACH posture the netns itself produces.
            rules.add(Arrays.asList(iptablesBinary, op, "OUTPUT",
                    "-m", "owner", "--uid-owner", "firecracker",
                    "-j", "REJECT"));
            return rules;
        }

        /** Spawn {@code argv}, wait up to 2s, return the exit code. A
         *  spawn or timeout failure logs and returns a non-zero rc so
         *  the caller can decide to fail-soft (destroy) or fail-loud
         *  (create). */
        private int run(List<String> argv) {
            try {
                Process p = processFactory.apply(argv);
                if (!p.waitFor(2, TimeUnit.SECONDS)) {
                    p.destroyForcibly();
                    log.warn("[netns] timed out: {}", argv);
                    return 124; // mimic GNU timeout(1)
                }
                return p.exitValue();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return 130;
            } catch (RuntimeException re) {
                log.warn("[netns] spawn failed argv={}: {}", argv, re.getMessage());
                return 127;
            }
        }
    }
}
