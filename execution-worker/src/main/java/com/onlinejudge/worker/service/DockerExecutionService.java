package com.onlinejudge.worker.service;

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

    private static final Map<String, String[]> LANGUAGE_CONFIGS = Map.of(
        "python", new String[]{"python:3.12-slim", "python3", "/code/solution.py"},
        "java",   new String[]{"openjdk:21-slim",  "sh", "-c", "cd /code && javac Solution.java && java Solution"},
        "cpp",    new String[]{"gcc:13",            "sh", "-c", "g++ -O2 -o /code/a.out /code/solution.cpp && /code/a.out"}
    );

    public ExecutionResult execute(String submissionId, String language, String code, String input) {
        String image = LANGUAGE_CONFIGS.getOrDefault(language, LANGUAGE_CONFIGS.get("python"))[0];
        String ext   = languageExtension(language);

        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("oj-" + submissionId);
            Path codeFile = tempDir.resolve("solution." + ext);
            Files.writeString(codeFile, code);
            Path inputFile = tempDir.resolve("input.txt");
            Files.writeString(inputFile, input != null ? input : "");

            List<String> cmd = buildDockerCommand(image, language, tempDir.toString());
            long startMs = System.currentTimeMillis();

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectInput(inputFile.toFile());
            pb.redirectErrorStream(true);
            Process process = pb.start();

            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
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
    List<String> buildDockerCommand(String image, String language, String hostDir) {
        String[] langCfg = LANGUAGE_CONFIGS.getOrDefault(language, LANGUAGE_CONFIGS.get("python"));
        List<String> cmd = new ArrayList<>(List.of(
            "docker", "run",
            "--rm",                                // destroy-never-reuse
            "--network", "none",                   // air-gapped network
            "--memory", memoryLimitMb + "m",       // memory.max equivalent
            "--cpus", "0.5",                       // cpu.max equivalent
            "--pids-limit", "64",                  // pids.max: fork bomb defense
            "--read-only",                         // immutable filesystem
            "--tmpfs", "/tmp:size=64m"
        ));

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
            image,
            langCfg[1], langCfg.length > 3 ? langCfg[2] + " " + langCfg[3] : langCfg[2]
        ));
        return cmd;
    }

    static boolean isLinuxHost() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return os.contains("linux");
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

    public record ExecutionResult(String status, String output, int executionTimeMs, int memoryUsedMb) {}
}
