package com.onlinejudge.worker.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes submitted code in a Docker container.
 *
 * Production gap: this uses Docker (shared Linux kernel) instead of
 * Firecracker MicroVMs (hardware-isolated guest kernel). The security
 * boundary is weaker. For trusted local development this is acceptable.
 *
 * Docker resource constraints approximate cgroup v2:
 *   --memory     = cgroup memory.max
 *   --cpus       = cgroup cpu.max
 *   --pids-limit = cgroup pids.max (fork bomb defense)
 *   --network none = air-gapped network
 *   --read-only    = immutable filesystem (except /tmp)
 *
 * Language images: lightweight official images (python:3.12-slim, gcc:13, etc.)
 * Destroy-never-reuse: --rm flag removes the container after execution.
 */
@Slf4j
@Service
public class DockerExecutionService {

    @Value("${app.execution.timeout-seconds:5}")
    private int timeoutSeconds;

    @Value("${app.execution.memory-limit-mb:256}")
    private int memoryLimitMb;

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

    private List<String> buildDockerCommand(String image, String language, String hostDir) {
        String[] langCfg = LANGUAGE_CONFIGS.getOrDefault(language, LANGUAGE_CONFIGS.get("python"));
        return List.of(
            "docker", "run",
            "--rm",                                // destroy-never-reuse
            "--network", "none",                   // air-gapped network
            "--memory", memoryLimitMb + "m",       // memory.max equivalent
            "--cpus", "0.5",                       // cpu.max equivalent
            "--pids-limit", "64",                  // pids.max: fork bomb defense
            "--read-only",                         // immutable filesystem
            "--tmpfs", "/tmp:size=64m",
            "-v", hostDir + ":/code:ro",
            image,
            langCfg[1], langCfg.length > 3 ? langCfg[2] + " " + langCfg[3] : langCfg[2]
        );
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
