package com.onlinejudge.sandbox.service;

import com.onlinejudge.sandbox.model.Sandbox;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CgroupApplier}.
 *
 * <p>The dev-mode test runs everywhere — it asserts that, on a host
 * without cgroup v2, the applier short-circuits and returns null
 * without throwing.
 *
 * <p>The "write the right values" test is gated by
 * {@link Assumptions#assumeTrue} on the presence of
 * {@code /sys/fs/cgroup/cgroup.controllers} — only Linux CI bots with
 * cgroup v2 enabled run the body. The applier writes to a per-test
 * temp dir (not the real {@code /sys/fs/cgroup/oj-sandbox/}) so we
 * never need real privileges or live sandboxes.
 */
class CgroupApplierTest {

    @Test
    void devMode_skipsWhenCgroupV2NotPresent() {
        // Force "not present" by pointing at a path that doesn't exist.
        // Effective only if the host itself lacks cgroup v2 — the
        // isAvailable() check inspects the well-known global file.
        CgroupApplier applier = new CgroupApplier("/nonexistent/oj-sandbox");
        Sandbox sb = new Sandbox("sb-dev", "python", 999L,
                "/tmp/x.sock", "/tmp/x-vsock.sock", "tok");
        if (applier.isAvailable()) {
            // Linux CI box — the dev-mode assertion doesn't apply.
            return;
        }
        String path = applier.applyAtLease(sb, 64L * 1024 * 1024, 100_000L, 100_000L);
        assertThat(path).isNull();
        // Cleanup is also a no-op.
        applier.cleanup(sb);
    }

    @Test
    void linuxOnly_writesMemoryCpuPidsAndProcs(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(
                Files.exists(Paths.get("/sys/fs/cgroup/cgroup.controllers")),
                "cgroup v2 not available — skipping");

        // Write to a temp dir we control. We can't actually move a pid
        // into a non-cgroupfs directory and have the kernel honor it,
        // but the applier's writes are file writes — verifying the
        // bytes lets us catch encoding bugs without root.
        CgroupApplier applier = new CgroupApplier(tmp.toString());
        Sandbox sb = new Sandbox("sb-1", "python", 12345L,
                "/tmp/x.sock", "/tmp/x-vsock.sock", "tok");

        String path = applier.applyAtLease(sb, 128L * 1024 * 1024,
                50_000L, 100_000L);
        // In temp-dir mode this is a regular path, not a cgroupfs path.
        assertThat(path).isNotNull();

        Path dir = Paths.get(path);
        assertThat(Files.readString(dir.resolve("memory.max")).trim())
                .isEqualTo(Long.toString(128L * 1024 * 1024));
        assertThat(Files.readString(dir.resolve("cpu.max")).trim())
                .isEqualTo("50000 100000");
        assertThat(Files.readString(dir.resolve("pids.max")).trim())
                .isEqualTo("64");
        assertThat(Files.readString(dir.resolve("cgroup.procs")).trim())
                .isEqualTo("12345");
        assertThat(sb.cgroupPath()).isEqualTo(path);

        // cleanup() walks and rmdirs — on a temp dir the file children
        // remain (cleanup only deletes directories), but the leaf dir
        // would only be removed if empty. We assert it doesn't throw.
        applier.cleanup(sb);
    }
}
