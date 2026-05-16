package com.onlinejudge.worker.service;

import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes submitted code in a Docker container.
 *
 * <p>Cross-platform default sandbox. Selected when {@code app.sandbox.backend=docker}
 * (the default). On macOS this is the only path that runs end-to-end.
 *
 * <p>Docker resource constraints approximate cgroup v2:
 * <pre>
 *   --memory     = cgroup memory.max
 *   --cpus       = cgroup cpu.max
 *   --pids-limit = cgroup pids.max (fork bomb defense)
 *   --network none = air-gapped network
 *   --read-only    = immutable filesystem (except /tmp)
 * </pre>
 *
 * <p><b>Linux hardening (opt-in).</b> Set {@code app.sandbox.linux-hardening.enabled=true}
 * on a Linux host and the {@link #buildDockerCommand} chain adds:
 * <ul>
 *   <li>{@code --security-opt seccomp=<profile.json>} — kernel filters all syscalls
 *       through a Seccomp-BPF program. Profile lives in {@code infra/seccomp/sandbox-seccomp.json}.</li>
 *   <li>{@code --security-opt no-new-privileges} — process can't acquire new privileges
 *       via {@code execve} (defeats setuid escalation).</li>
 *   <li>{@code --cap-drop=ALL} — drops every Linux capability. Code execution
 *       doesn't need any of them.</li>
 *   <li>{@code --cgroupns=private} — private cgroup namespace, so the container
 *       can't observe the host's cgroup hierarchy.</li>
 * </ul>
 * These flags are silently skipped on non-Linux hosts (Docker for Mac doesn't
 * implement them anyway). Together they close the cgroup v2 + Seccomp-BPF gap
 * the blog calls out in Part 7 — same kernel facilities Firecracker uses
 * internally, just applied to a container.
 *
 * <p>For full hardware isolation see {@link FirecrackerExecutionService}.
 *
 * <p><b>Runtime selection (gVisor).</b> The OCI runtime Docker hands the
 * container to is configurable via {@code app.sandbox.docker.runtime}:
 * <ul>
 *   <li>{@code runc} (default) — the standard runtime; relies on namespaces +
 *       cgroups + (when hardening is on) Seccomp-BPF.</li>
 *   <li>{@code runsc} — Google's gVisor; intercepts guest syscalls in
 *       user-space and replays a tiny vetted subset against the host kernel.
 *       Stronger isolation than runc, modest perf hit. Requires runsc to be
 *       registered with the Docker daemon via {@code /etc/docker/daemon.json}.</li>
 * </ul>
 * When the configured runtime is not {@code runc}, a {@code --runtime=<name>}
 * flag is added to the {@code docker run} argv. The flag is omitted for
 * {@code runc} so unit tests + the macOS dev loop don't break (Docker for
 * Mac ships only runc).
 *
 * <p>Language images: lightweight official images (python:3.12-slim, gcc:13, etc.)
 * Destroy-never-reuse: {@code --rm} flag removes the container after execution.
 */
@Slf4j
@Service
@Primary
@ConditionalOnProperty(name = "app.sandbox.backend", havingValue = "docker", matchIfMissing = true)
public class DockerExecutionService implements ExecutionBackend {

    @Value("${app.execution.timeout-seconds:5}")
    private int timeoutSeconds;

    @Value("${app.execution.memory-limit-mb:256}")
    private int memoryLimitMb;

    /** Linux-only hardening: Seccomp-BPF + capability drop + cgroup namespace. */
    @Value("${app.sandbox.linux-hardening.enabled:false}")
    private boolean linuxHardeningEnabled;

    /** Absolute path to the Seccomp-BPF profile JSON (Linux only). */
    @Value("${app.sandbox.seccomp-profile:/etc/seccomp/sandbox-seccomp.json}")
    private String seccompProfilePath;

    /**
     * OCI runtime Docker should use for the contestant container. Default
     * {@code runc}; set to {@code runsc} on a Linux host with gVisor installed
     * + registered in {@code /etc/docker/daemon.json}. Any non-{@code runc}
     * value is passed verbatim as {@code --runtime=<value>}.
     */
    @Value("${app.sandbox.docker.runtime:runc}")
    private String dockerRuntime;

    private record LanguageConfig(String image, String sourceFile, List<String> command) {}

    private static final Map<String, LanguageConfig> LANGUAGE_CONFIGS = Map.of(
        "python", new LanguageConfig("python:3.12-slim", "solution.py",
                List.of("python3", "/code/solution.py")),
        "java", new LanguageConfig("openjdk:21-slim", "Solution.java",
                List.of("sh", "-c", "javac -d /tmp /code/Solution.java && java -cp /tmp Solution")),
        "cpp", new LanguageConfig("gcc:13", "solution.cpp",
                List.of("sh", "-c", "g++ -O2 -pipe -o /tmp/a.out /code/solution.cpp && /tmp/a.out"))
    );

    @Override
    public ExecutionResult execute(String submissionId, String language, String code,
                                   List<TestCaseSpec> testCases,
                                   int timeLimitMs, int memoryLimitMb) {
        // Docker backend respects the per-problem limits when supplied
        // (roadmap §2.3). 0 means "fall back to the application.yml
        // default" — that path is also exercised by the smoke / bypass
        // flow where problem-service is intentionally not deployed.
        int effectiveTimeoutSec = timeLimitMs > 0
                ? Math.max(1, (timeLimitMs + 999) / 1000)
                : timeoutSeconds;
        int effectiveMemoryMb = memoryLimitMb > 0 ? memoryLimitMb : this.memoryLimitMb;
        return executeInternal(submissionId, language, code, testCases,
                effectiveTimeoutSec, effectiveMemoryMb);
    }

    private ExecutionResult executeInternal(String submissionId, String language, String code,
                                            List<TestCaseSpec> testCases,
                                            int effectiveTimeoutSec, int effectiveMemoryMb) {
        // Docker backend runs the first test case only — it's the dev / smoke
        // fallback when Firecracker isn't available. Multi-test orchestration
        // lives in the Firecracker path (which ships all cases over vsock in a
        // single agent round-trip).
        String input = (testCases == null || testCases.isEmpty())
                ? ""
                : (testCases.get(0).input() == null ? "" : testCases.get(0).input());

        LanguageConfig cfg = languageConfig(language);

        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("oj-" + submissionId);
            Path codeFile = tempDir.resolve(cfg.sourceFile());
            Files.writeString(codeFile, code);
            Path inputFile = tempDir.resolve("input.txt");
            Files.writeString(inputFile, input);

            List<String> cmd = buildDockerCommand(cfg.image(), language,
                    tempDir.toString(), effectiveMemoryMb);
            long startMs = System.currentTimeMillis();

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectInput(inputFile.toFile());
            pb.redirectErrorStream(true);
            Process process = pb.start();

            boolean finished = process.waitFor(effectiveTimeoutSec, TimeUnit.SECONDS);
            long execMs = System.currentTimeMillis() - startMs;

            if (!finished) {
                process.destroyForcibly();
                log.warn("[worker] TLE submission={} lang={} time={}ms", submissionId, language, execMs);
                return new ExecutionResult("TIME_LIMIT_EXCEEDED", "", (int) execMs, 0);
            }

            String output = new String(process.getInputStream().readAllBytes()).trim();
            int exitCode = process.exitValue();

            if (exitCode != 0) {
                return new ExecutionResult("RUNTIME_ERROR", output, (int) execMs, 0);
            }

            return new ExecutionResult("OK", output, (int) execMs, 0);

        } catch (IOException | InterruptedException ex) {
            log.error("[worker] Execution error submission={}: {}", submissionId, ex.getMessage());
            return new ExecutionResult("INTERNAL_ERROR", ex.getMessage(), 0, 0);
        } finally {
            cleanup(tempDir);
        }
    }

    /**
     * Builds the {@code docker run …} argv. Hardening flags are applied only
     * when both the config switch is on AND the host kernel is Linux —
     * Docker on macOS/Windows runs a Linux VM but its host-side flags
     * (seccomp, cgroupns) are inherited inside the VM, so the safest behavior
     * elsewhere is to skip the flag rather than risk a mis-configured container.
     */
    /** Test seam — legacy signature uses the configured default memory cap. */
    List<String> buildDockerCommand(String image, String language, String hostDir) {
        return buildDockerCommand(image, language, hostDir, memoryLimitMb);
    }

    List<String> buildDockerCommand(String image, String language, String hostDir, int memCapMb) {
        LanguageConfig langCfg = languageConfig(language);
        List<String> cmd = new ArrayList<>(List.of(
            "docker", "run",
            "--rm",                                // destroy-never-reuse
            "--network", "none",                   // air-gapped network
            "--memory", memCapMb + "m",            // memory.max equivalent
            "--cpus", "0.5",                       // cpu.max equivalent
            "--pids-limit", "64",                  // pids.max: fork bomb defense
            "--read-only",                         // immutable filesystem
            "--tmpfs", "/tmp:size=64m"
        ));

        // OCI runtime selection. Omitted for runc so dev machines without
        // gVisor (Docker for Mac, CI runners) keep working unchanged. Any
        // non-runc value is passed verbatim — Docker rejects unknown runtimes
        // with a clear error if the daemon hasn't registered it.
        if (dockerRuntime != null && !dockerRuntime.isBlank()
                && !"runc".equalsIgnoreCase(dockerRuntime)) {
            cmd.add("--runtime");
            cmd.add(dockerRuntime);
            log.debug("[sandbox] Docker runtime override: --runtime={}", dockerRuntime);
        }

        if (linuxHardeningEnabled && isLinuxHost()) {
            cmd.addAll(List.of(
                    "--security-opt", "seccomp=" + seccompProfilePath,
                    "--security-opt", "no-new-privileges",
                    "--cap-drop", "ALL",
                    "--cgroupns", "private"
            ));
            log.debug("[sandbox] Linux hardening flags applied: seccomp+caps+cgroupns");
        } else if (linuxHardeningEnabled) {
            log.debug("[sandbox] Linux hardening requested but host is {}; skipping flags",
                    System.getProperty("os.name"));
        }

        cmd.addAll(List.of(
            "-v", hostDir + ":/code:ro",
            image
        ));
        cmd.addAll(langCfg.command());
        return cmd;
    }

    static boolean isLinuxHost() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return os.contains("linux");
    }

    private static LanguageConfig languageConfig(String language) {
        return LANGUAGE_CONFIGS.getOrDefault(language, LANGUAGE_CONFIGS.get("python"));
    }

    private void cleanup(Path dir) {
        if (dir == null) return;
        try {
            Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
    }

    // The previous wrapper class `DockerExecutionService.ExecutionResult` was
    // removed because it shadowed the interface record `ExecutionBackend.ExecutionResult`
    // in this scope, breaking @Override on execute(...). Tests now reference
    // the interface record directly: `new ExecutionBackend.ExecutionResult(...)`.
}
