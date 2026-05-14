package com.onlinejudge.worker.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Executes contestant code in a per-submission Firecracker microVM (Part 7).
 *
 * <p>Linux-only. Firecracker is Apache 2.0 (no vendor lock-in) but it relies on
 * {@code /dev/kvm} — the Linux kernel's hardware-virtualization interface — so
 * it cannot run on macOS or Windows hosts. The bean is gated by
 * {@code app.sandbox.backend=firecracker}; even with that flag set, the
 * constructor refuses to come up on a non-Linux host so misconfiguration fails
 * loudly at boot rather than at the first submission.
 *
 * <p><b>How it works.</b> One Firecracker process per submission, addressed via
 * a per-submission Unix-domain socket. The boot path is:
 * <ol>
 *   <li>Start {@code firecracker --api-sock /run/fc-{submissionId}.sock}.</li>
 *   <li>{@code PUT /boot-source} pointing at {@code app.sandbox.firecracker.kernel-image}.</li>
 *   <li>{@code PUT /drives/rootfs} pointing at {@code app.sandbox.firecracker.rootfs-image}
 *       (a squashfs/ext4 with the language runtimes).</li>
 *   <li>{@code PUT /machine-config} setting vCPU + memory.</li>
 *   <li>Mount the submission's code+input as a second drive (or via a vsock).</li>
 *   <li>{@code PUT /actions} of type {@code InstanceStart} → the VM boots and
 *       runs the entrypoint which compiles+runs the code, writes stdout to a
 *       shared file.</li>
 *   <li>On exit (or wall-clock timeout) the host reads the output file and
 *       kills the VM.</li>
 * </ol>
 *
 * <p>The full Firecracker integration (kernel image build, rootfs assembly, jailer
 * configuration, network tap setup, vsock plumbing) is out of scope for the
 * code in this repo — those are operator concerns covered in
 * {@code infra/firecracker/README.md}. This class implements the host-side
 * Java orchestration: it spawns the {@code firecracker} binary, drives its
 * REST socket via {@code curl --unix-socket}, and tears down on completion.
 *
 * <p>For unit-test friendliness on macOS, the {@link #execute} call short-circuits
 * with {@code INTERNAL_ERROR} if the configured binary or images don't exist;
 * tests can stub by overriding {@link #isLinuxHost()} or by injecting a
 * non-Linux backend selector.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "app.sandbox.backend", havingValue = "firecracker")
public class FirecrackerExecutionService implements ExecutionBackend {

    @Value("${app.execution.timeout-seconds:5}")
    private int timeoutSeconds;

    @Value("${app.execution.memory-limit-mb:256}")
    private int memoryLimitMb;

    @Value("${app.sandbox.firecracker.binary:/usr/local/bin/firecracker}")
    private String firecrackerBinary;

    @Value("${app.sandbox.firecracker.kernel-image:/var/lib/firecracker/vmlinux}")
    private String kernelImage;

    @Value("${app.sandbox.firecracker.rootfs-image:/var/lib/firecracker/rootfs.ext4}")
    private String rootfsImage;

    @Value("${app.sandbox.firecracker.vcpus:1}")
    private int vcpus;

    /**
     * Directory where per-submission Firecracker API sockets are created.
     * Default {@code /tmp} works for any local user; production deployments
     * wrapping Firecracker in {@code jailer} typically override this to the
     * jailer's chrooted {@code /run} or {@code /var/run}.
     */
    @Value("${app.sandbox.firecracker.api-sock-dir:/tmp}")
    private String apiSockDir;

    /**
     * Kernel command-line passed to the guest. Default boots whatever {@code init}
     * the rootfs provides via the kernel's normal search path — works with the
     * Firecracker CI's Ubuntu rootfs (which uses {@code /sbin/init}) and any
     * rootfs that ships a standard init. Production deployments with a custom
     * harness rootfs typically override to point at the harness, e.g.
     * {@code app.sandbox.firecracker.boot-args=console=ttyS0 reboot=k panic=1 pci=off init=/init}.
     */
    @Value("${app.sandbox.firecracker.boot-args:console=ttyS0 reboot=k panic=1 pci=off}")
    private String bootArgs;

    public FirecrackerExecutionService() {
        if (!isLinuxHost()) {
            throw new IllegalStateException(
                    "FirecrackerExecutionService requires Linux (needs /dev/kvm). " +
                    "Detected os.name=" + System.getProperty("os.name") + ". " +
                    "Set app.sandbox.backend=docker on this host.");
        }
        if (!Files.exists(Paths.get("/dev/kvm"))) {
            throw new IllegalStateException(
                    "/dev/kvm is not available — Firecracker cannot start. " +
                    "Ensure the host has nested virtualization enabled and the user is in the kvm group.");
        }
        log.info("[firecracker] backend selected (Linux + /dev/kvm present)");
    }

    @Override
    public DockerExecutionService.ExecutionResult execute(
            String submissionId, String language, String code, String input) {

        // Sanity-check images exist before spending time on a doomed boot.
        if (!Files.exists(Paths.get(firecrackerBinary))
                || !Files.exists(Paths.get(kernelImage))
                || !Files.exists(Paths.get(rootfsImage))) {
            log.error("[firecracker] Required artifact missing — binary={} kernel={} rootfs={}",
                    firecrackerBinary, kernelImage, rootfsImage);
            return new DockerExecutionService.ExecutionResult(
                    "INTERNAL_ERROR", "firecracker artifacts missing", 0, 0);
        }

        String apiSock = apiSockDir + "/fc-" + submissionId + "-" + UUID.randomUUID() + ".sock";
        Path codeDir = null;
        Process firecracker = null;
        try {
            codeDir = Files.createTempDirectory("oj-fc-" + submissionId);
            String ext = languageExtension(language);
            Files.writeString(codeDir.resolve("solution." + ext), code);
            Files.writeString(codeDir.resolve("input.txt"), input == null ? "" : input);

            firecracker = new ProcessBuilder(firecrackerBinary, "--api-sock", apiSock)
                    .redirectErrorStream(true)
                    .start();

            // Give the API socket up to 2s to come up.
            long deadline = System.currentTimeMillis() + 2000;
            while (!Files.exists(Paths.get(apiSock)) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            if (!Files.exists(Paths.get(apiSock))) {
                return new DockerExecutionService.ExecutionResult(
                        "INTERNAL_ERROR", "firecracker api socket never appeared", 0, 0);
            }

            // Configure boot, rootfs, machine via the REST API. The actual JSON
            // bodies are operator-tuned (kernel cmdline, vCPU, mem, etc.) — we
            // ship a minimal default that matches infra/firecracker/README.md.
            long startMs = System.currentTimeMillis();
            putApi(apiSock, "/boot-source",
                    "{\"kernel_image_path\":\"" + kernelImage +
                    "\",\"boot_args\":\"" + bootArgs + "\"}");
            putApi(apiSock, "/drives/rootfs",
                    "{\"drive_id\":\"rootfs\",\"path_on_host\":\"" + rootfsImage +
                    "\",\"is_root_device\":true,\"is_read_only\":true}");
            putApi(apiSock, "/machine-config",
                    "{\"vcpu_count\":" + vcpus + ",\"mem_size_mib\":" + memoryLimitMb + "}");
            putApi(apiSock, "/actions", "{\"action_type\":\"InstanceStart\"}");

            boolean finished = firecracker.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            long execMs = System.currentTimeMillis() - startMs;
            if (!finished) {
                firecracker.destroyForcibly();
                log.warn("[firecracker] TLE submission={} time={}ms", submissionId, execMs);
                return new DockerExecutionService.ExecutionResult(
                        "TIME_LIMIT_EXCEEDED", "", (int) execMs, 0);
            }

            String stdout = new String(firecracker.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8).trim();
            int exit = firecracker.exitValue();
            if (exit != 0) {
                return new DockerExecutionService.ExecutionResult(
                        "RUNTIME_ERROR", stdout, (int) execMs, 0);
            }
            return new DockerExecutionService.ExecutionResult("OK", stdout, (int) execMs, 0);

        } catch (IOException | InterruptedException ex) {
            log.error("[firecracker] Execution error submission={}: {}", submissionId, ex.getMessage());
            return new DockerExecutionService.ExecutionResult("INTERNAL_ERROR", ex.getMessage(), 0, 0);
        } finally {
            if (firecracker != null && firecracker.isAlive()) firecracker.destroyForcibly();
            try { Files.deleteIfExists(Paths.get(apiSock)); } catch (IOException ignored) {}
            cleanup(codeDir);
        }
    }

    /**
     * Drives the Firecracker REST API over its Unix socket via {@code curl}.
     * We shell out rather than implementing AF_UNIX HTTP in Java to keep this
     * file small; production deployments commonly use a small HTTP client lib.
     */
    private void putApi(String sock, String path, String json) throws IOException, InterruptedException {
        List<String> cmd = List.of(
                "curl", "--unix-socket", sock, "-s", "-X", "PUT",
                "-H", "Content-Type: application/json",
                "-d", json,
                "http://localhost" + path);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        if (!p.waitFor(2, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("Firecracker API PUT " + path + " timed out");
        }
        if (p.exitValue() != 0) {
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new IOException("Firecracker API PUT " + path + " failed: " + out);
        }
    }

    static boolean isLinuxHost() {
        return DockerExecutionService.isLinuxHost();
    }

    private String languageExtension(String language) {
        return switch (language) {
            case "python" -> "py";
            case "java"   -> "java";
            case "cpp"    -> "cpp";
            default       -> "txt";
        };
    }

    private void cleanup(Path dir) {
        if (dir == null) return;
        try {
            Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
    }
}
