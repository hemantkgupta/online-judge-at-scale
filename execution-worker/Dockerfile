# =============================================================================
# execution-worker — Spring Boot fat jar in a slim JRE, plus the host-side
# tooling the sandbox backends need:
#   * docker CLI: DockerExecutionService spawns sibling containers via the
#     mounted /var/run/docker.sock from the host.
#   * curl:       FirecrackerExecutionService drives the VMM REST socket.
#
# The container itself must be run with --privileged (or specific caps +
# devices) on the host so the worker can use /dev/kvm and the Docker socket.
# See infra/gcp/compose/compute-compose.yml for the exact run-time settings.
#
# Build (from the repo root):
#   docker buildx build \
#     --platform=linux/amd64 \
#     -f execution-worker/Dockerfile \
#     -t asia-south1-docker.pkg.dev/online-judge-hk/oj-images/execution-worker:latest \
#     --push .
# =============================================================================

# ---- Stage 1: build the fat jar -------------------------------------------
FROM gradle:8.7-jdk17 AS builder

WORKDIR /build

COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY common/build.gradle             common/
COPY api-gateway/build.gradle        api-gateway/
COPY execution-worker/build.gradle   execution-worker/
COPY scoring-pipeline/build.gradle   scoring-pipeline/
COPY leaderboard-service/build.gradle leaderboard-service/
COPY analytics-pipeline/build.gradle analytics-pipeline/
COPY contest-service/build.gradle    contest-service/
COPY problem-service/build.gradle    problem-service/
COPY common/src/main/proto           common/src/main/proto/

RUN gradle :execution-worker:dependencies --no-daemon --refresh-dependencies || true

COPY common/src           common/src/
COPY execution-worker/src execution-worker/src/

RUN gradle :execution-worker:bootJar --no-daemon -x test


# ---- Stage 2: runtime ------------------------------------------------------
FROM eclipse-temurin:17-jre-jammy

# docker.io ships the CLI (no daemon). curl + ca-certificates for HTTPS and
# the Firecracker REST socket dance.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        docker.io curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/execution-worker/build/libs/execution-worker-*.jar /app/app.jar

ENV JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:InitialRAMPercentage=30 -XX:+ExitOnOutOfMemoryError"

# Health endpoint port. Worker has no HTTP traffic of its own beyond actuator.
EXPOSE 8081

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
