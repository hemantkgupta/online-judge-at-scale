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


# ---- Stage 2: build the host-side vsock-client ----------------------------
# Per Workstream E of the production rollout: the worker now dials the
# in-guest Execution Agent directly over Firecracker's vsock so the
# Sandbox Manager stays out of the data plane carrying contestant code.
# The JVM has no native AF_VSOCK; the same tiny Go bridge binary the SM
# uses (oj-vsock-client) is baked into this image so the worker can
# ProcessBuilder it and pipe JSON over its stdio.
FROM golang:1.22 AS go-builder

WORKDIR /agent
COPY infra/firecracker/agent/go.mod infra/firecracker/agent/go.sum ./
RUN go mod download
COPY infra/firecracker/agent/cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/oj-vsock-client ./cmd/vsock-client


# ---- Stage 3: runtime ------------------------------------------------------
FROM eclipse-temurin:17-jre-jammy

# docker.io ships the CLI for the local Docker backend (sibling-container
# spawning on the host). curl + ca-certificates for the actuator/probe and
# any outbound HTTPS. NOTE: Firecracker / mke2fs / debugfs are GONE from
# this image — the privileged sandbox lifecycle moved out into the
# sandbox-manager container per Part 7's trust-zone split.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        docker.io curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# OpenTelemetry Java agent (Workstream H). The agent auto-instruments
# Spring Boot / Kafka / JDBC / HTTP client on JVM startup and exports
# OTLP/gRPC to whatever OTEL_EXPORTER_OTLP_ENDPOINT is set to at runtime.
# When that env var is unset the agent still loads but exports nowhere —
# i.e. a fully no-op deployment if no collector is provisioned.
#
# OTEL_SERVICE_NAME + OTEL_RESOURCE_ATTRIBUTES are set per-service via
# compose (see infra/gcp/compose/compute-compose.yml).
RUN curl -fsSL -o /opt/opentelemetry-javaagent.jar \
    https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.10.0/opentelemetry-javaagent.jar

ENV JAVA_TOOL_OPTIONS="-javaagent:/opt/opentelemetry-javaagent.jar"
ENV OTEL_EXPORTER_OTLP_PROTOCOL=grpc
# We don't ship logs via OTel yet (logs go to stdout → docker logs).
ENV OTEL_LOGS_EXPORTER=none

WORKDIR /app

COPY --from=builder    /build/execution-worker/build/libs/execution-worker-*.jar /app/app.jar
COPY --from=go-builder /out/oj-vsock-client                                      /usr/local/bin/oj-vsock-client

ENV JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:InitialRAMPercentage=30 -XX:+ExitOnOutOfMemoryError"

# Health endpoint port. Worker has no HTTP traffic of its own beyond actuator.
EXPOSE 8081

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
