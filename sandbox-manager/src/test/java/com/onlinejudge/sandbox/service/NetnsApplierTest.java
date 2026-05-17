package com.onlinejudge.sandbox.service;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NetnsApplier.DefaultNetnsApplier}.
 *
 * <p>These tests never spawn a real {@code ip} or {@code iptables}
 * binary — they install an in-memory {@link Function} that captures
 * the argv and returns a fake {@link Process} with a chosen exit code.
 * That lets the same suite run on macOS dev boxes without root.
 */
class NetnsApplierTest {

    /** Stub Process with a fixed exit code and stdout payload. */
    static final class FakeProcess extends Process {
        final int rc;
        final byte[] out;
        FakeProcess(int rc, String stdout) {
            this.rc = rc;
            this.out = stdout.getBytes();
        }
        @Override public OutputStream getOutputStream() { return OutputStream.nullOutputStream(); }
        @Override public InputStream getInputStream()   { return new ByteArrayInputStream(out); }
        @Override public InputStream getErrorStream()   { return InputStream.nullInputStream(); }
        @Override public int waitFor() { return rc; }
        @Override public boolean waitFor(long t, TimeUnit u) { return true; }
        @Override public int exitValue() { return rc; }
        @Override public void destroy() {}
        @Override public Process destroyForcibly() { return this; }
        @Override public boolean isAlive() { return false; }
    }

    /** Captures argv lists handed to {@link ProcessBuilder} and emits
     *  a fake {@link Process} per a per-prefix lookup. */
    static final class RecordingFactory implements Function<List<String>, Process> {
        final List<List<String>> calls = new ArrayList<>();
        /** Mutable so tests can rebind a dispatch that reads back into
         *  {@link #calls} (you can't reference {@code this} from the
         *  ctor arg). */
        Function<List<String>, FakeProcess> dispatch;
        RecordingFactory(Function<List<String>, FakeProcess> dispatch) {
            this.dispatch = dispatch;
        }
        @Override public Process apply(List<String> argv) {
            calls.add(argv);
            return dispatch.apply(argv);
        }
    }

    private NetnsApplier.DefaultNetnsApplier newApplier(RecordingFactory f) {
        NetnsApplier.DefaultNetnsApplier a = new NetnsApplier.DefaultNetnsApplier();
        a.setProcessFactory(f);
        return a;
    }

    // ---------------------------------------------------------------

    @Test
    void create_invokesIpNetnsAddWithExpectedArgv() {
        RecordingFactory factory = new RecordingFactory(argv -> new FakeProcess(0, ""));
        NetnsApplier.DefaultNetnsApplier a = newApplier(factory);

        a.create("sb-abc");

        // First call: `ip netns add oj-sb-abc`.
        assertThat(factory.calls.get(0))
                .containsExactly("/usr/sbin/ip", "netns", "add", "oj-sb-abc");
        // Second call: bring lo up inside the namespace.
        assertThat(factory.calls.get(1))
                .containsExactly("/usr/sbin/ip", "netns", "exec", "oj-sb-abc",
                        "/usr/sbin/ip", "link", "set", "lo", "up");
    }

    @Test
    void create_retriesAfterDeleteOnExistingNetns() {
        // First add fails (rc=1, "File exists"); del + retry succeeds.
        // RecordingFactory.dispatch is rewritten after construction so
        // the lambda can read the live call list and only fail the
        // FIRST add.
        RecordingFactory factory = new RecordingFactory(argv -> new FakeProcess(0, ""));
        factory.dispatch = argv -> {
            long addCount = factory.calls.stream()
                    .filter(c -> c.size() >= 3 && c.get(2).equals("add"))
                    .count();
            if (argv.size() >= 3 && argv.get(2).equals("add") && addCount <= 1) {
                return new FakeProcess(1, "File exists");
            }
            return new FakeProcess(0, "");
        };
        NetnsApplier.DefaultNetnsApplier a = newApplier(factory);

        a.create("sb-rerun");

        // 4 calls: first add (fails), del, retry add (succeeds), then
        // `ip netns exec <n> ip link set lo up`.
        assertThat(factory.calls).hasSize(4);
        assertThat(factory.calls.get(0)).contains("add");
        assertThat(factory.calls.get(1)).contains("del");
        assertThat(factory.calls.get(2)).contains("add");
        assertThat(factory.calls.get(3)).contains("link");
    }

    @Test
    void destroy_isIdempotentOnMissingNetns() {
        // `ip netns del` returns rc=1 + "Cannot remove namespace ...";
        // destroy() must NOT throw.
        RecordingFactory factory = new RecordingFactory(
                argv -> new FakeProcess(1, "Cannot remove namespace file"));
        NetnsApplier.DefaultNetnsApplier a = newApplier(factory);

        a.destroy("sb-gone");

        assertThat(factory.calls).hasSize(1);
        assertThat(factory.calls.get(0))
                .containsExactly("/usr/sbin/ip", "netns", "del", "oj-sb-gone");
    }

    @Test
    void netnsName_trimsTo15Chars() {
        NetnsApplier.DefaultNetnsApplier a = newApplier(
                new RecordingFactory(argv -> new FakeProcess(0, "")));
        // "oj-" + 20 chars = 23 → trimmed to 15.
        String name = a.netnsName("abcdefghijklmnopqrst");
        assertThat(name).hasSize(15);
        assertThat(name).startsWith("oj-");
        // Short id passes through.
        assertThat(a.netnsName("xyz")).isEqualTo("oj-xyz");
    }

    @Test
    void execPrefix_returnsIpNetnsExecArgv() {
        NetnsApplier.DefaultNetnsApplier a = newApplier(
                new RecordingFactory(argv -> new FakeProcess(0, "")));
        assertThat(a.execPrefix("sb-1"))
                .containsExactly("/usr/sbin/ip", "netns", "exec", "oj-sb-1");
    }

    @Test
    void iptablesRules_disabledByDefault_noProcessSpawn() {
        RecordingFactory factory = new RecordingFactory(argv -> new FakeProcess(0, ""));
        NetnsApplier.DefaultNetnsApplier a = newApplier(factory);
        // iptablesEnabled defaults to false in DefaultNetnsApplier.
        a.installIptablesRules("sb-1");
        a.removeIptablesRules("sb-1");
        assertThat(factory.calls).isEmpty();
    }

    @Test
    void iptablesRules_whenEnabled_insertExpectedArgv() {
        RecordingFactory factory = new RecordingFactory(argv -> new FakeProcess(0, ""));
        NetnsApplier.DefaultNetnsApplier a = newApplier(factory);
        a.setIptablesEnabled(true);

        a.installIptablesRules("sb-X");

        assertThat(factory.calls).hasSize(2);
        // Rule 1: FORWARD drop for the tap.
        assertThat(factory.calls.get(0)).isEqualTo(Arrays.asList(
                "/usr/sbin/iptables", "-I", "FORWARD",
                "-i", "fc-tap-sb-X", "-j", "DROP"));
        // Rule 2: OUTPUT reject for the firecracker UID.
        assertThat(factory.calls.get(1)).isEqualTo(Arrays.asList(
                "/usr/sbin/iptables", "-I", "OUTPUT",
                "-m", "owner", "--uid-owner", "firecracker",
                "-j", "REJECT"));
    }

    @Test
    void iptablesRules_removeUsesDashD() {
        RecordingFactory factory = new RecordingFactory(argv -> new FakeProcess(0, ""));
        NetnsApplier.DefaultNetnsApplier a = newApplier(factory);
        a.setIptablesEnabled(true);

        a.removeIptablesRules("sb-X");

        assertThat(factory.calls.get(0)).contains("-D");
        assertThat(factory.calls.get(1)).contains("-D");
    }
}
