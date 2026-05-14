package com.onlinejudge.worker.service;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for {@link FirecrackerExecutionService}.
 *
 * <p>The class is Linux-only (needs {@code /dev/kvm}). The constructor refuses
 * to come up otherwise so that {@code app.sandbox.backend=firecracker} on a
 * macOS host fails loudly at boot rather than silently selecting a backend
 * that can't run anything.
 *
 * <p>Three flavors of test here:
 * <ul>
 *   <li>{@link #constructor_refusesNonLinuxHost()} — runs only on macOS/Windows,
 *       verifies the platform refusal.</li>
 *   <li>{@link #constructor_refusesLinuxHostWithoutKvm()} — runs only on Linux
 *       boxes that lack {@code /dev/kvm}, verifies the missing-KVM refusal.</li>
 *   <li>{@link #execute_bootsRealMicroVMAndCleanlyReturnsAfterTimeout()} — runs
 *       only when Firecracker is actually installable (Linux + {@code /dev/kvm}
 *       + the binary + the prebuilt artifacts), exercises the full
 *       {@code execute(...)} path against a real microVM. Skips on every
 *       laptop CI and runs on real-hardware boxes (Raspberry Pi 5, cloud
 *       Linux with nested virt, etc.).</li>
 * </ul>
 *
 * <p>Together these cover every platform path: refusal on the wrong host,
 * refusal when KVM is missing, and a real-hardware integration when Firecracker
 * is actually available. There is no host on which all three run.
 */
class FirecrackerExecutionServiceTest {

    @Test
    void constructor_refusesNonLinuxHost() {
        assumeTrue(!DockerExecutionService.isLinuxHost(),
                "Only runs on non-Linux hosts — verifies the macOS/Windows refusal path");

        assertThatThrownBy(FirecrackerExecutionService::new)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires Linux");
    }

    @Test
    void constructor_refusesLinuxHostWithoutKvm() {
        assumeTrue(DockerExecutionService.isLinuxHost(),
                "Only runs on Linux");
        assumeTrue(!Files.exists(Paths.get("/dev/kvm")),
                "Only runs on Linux hosts that lack /dev/kvm — verifies the missing-KVM refusal path");

        assertThatThrownBy(FirecrackerExecutionService::new)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("/dev/kvm");
    }

    /**
     * End-to-end host-side integration test: actually drives a real Firecracker
     * microVM through the same REST sequence
     * {@link FirecrackerExecutionService#execute} uses in production
     * ({@code /boot-source} → {@code /drives/rootfs} → {@code /machine-config}
     * → {@code /actions InstanceStart}).
     *
     * <p>The Firecracker CI's prebuilt rootfs has no {@code /init} harness that
     * runs contestant code and reboots, so the guest stays up forever and the
     * host's wall-clock kill ({@code process.waitFor(timeoutSeconds, …)})
     * fires. That's expected — what this test proves on real hardware is:
     * <ul>
     *   <li>the {@code firecracker} binary spawns and the API socket appears,</li>
     *   <li>the {@code curl --unix-socket} REST PUT sequence succeeds,</li>
     *   <li>{@code InstanceStart} is accepted by the VMM,</li>
     *   <li>the host-side watchdog kill fires within the configured timeout,</li>
     *   <li>the cleanup paths ({@code destroyForcibly}, socket delete, tempdir
     *       wipe) run without throwing,</li>
     *   <li>the {@link DockerExecutionService.ExecutionResult} is returned cleanly.</li>
     * </ul>
     *
     * <p>In production a custom rootfs with an {@code /init} that runs the
     * submission and calls {@code reboot} would let this reach
     * {@code status=OK} with the submission's stdout in {@code output}.
     */
    @Test
    void execute_bootsRealMicroVMAndCleanlyReturnsAfterTimeout() {
        assumeTrue(DockerExecutionService.isLinuxHost(), "Needs Linux");
        assumeTrue(Files.exists(Paths.get("/dev/kvm")), "Needs /dev/kvm");
        assumeTrue(Files.exists(Paths.get("/usr/local/bin/firecracker")),
                "Needs Firecracker binary at /usr/local/bin/firecracker");
        assumeTrue(Files.exists(Paths.get("/var/lib/firecracker/vmlinux")),
                "Needs guest kernel at /var/lib/firecracker/vmlinux");
        assumeTrue(Files.exists(Paths.get("/var/lib/firecracker/rootfs.ext4")),
                "Needs guest rootfs at /var/lib/firecracker/rootfs.ext4");

        FirecrackerExecutionService svc = new FirecrackerExecutionService();
        // The @Value fields aren't populated outside Spring — set them by
        // reflection to mirror the application.yml defaults.
        ReflectionTestUtils.setField(svc, "timeoutSeconds", 5);
        ReflectionTestUtils.setField(svc, "memoryLimitMb", 256);
        ReflectionTestUtils.setField(svc, "firecrackerBinary", "/usr/local/bin/firecracker");
        ReflectionTestUtils.setField(svc, "kernelImage", "/var/lib/firecracker/vmlinux");
        ReflectionTestUtils.setField(svc, "rootfsImage", "/var/lib/firecracker/rootfs.ext4");
        ReflectionTestUtils.setField(svc, "vcpus", 1);
        // /tmp is writable by any local user; the production deployment with
        // jailer would override this to the chrooted runtime dir.
        ReflectionTestUtils.setField(svc, "apiSockDir", "/tmp");

        DockerExecutionService.ExecutionResult result = svc.execute(
                "fc-test-" + System.currentTimeMillis(),
                "python", "print(42)", "");

        assertThat(result).isNotNull();
        assertThat(result.status())
                .as("Prebuilt rootfs has no /init harness, so wall-clock kill is the expected outcome.")
                .isEqualTo("TIME_LIMIT_EXCEEDED");
        // The kill fires at ~timeoutSeconds, plus a little spawn/IO overhead.
        assertThat(result.executionTimeMs()).isBetween(4_000, 15_000);
    }
}
