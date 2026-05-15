# =============================================================================
# sandbox-manager — Part 7's per-host privileged daemon. Owns Firecracker REST
# sockets, KVM, and the lease/exec/release HTTP API.
#
# Two builders + one runtime stage:
#   * Stage 1 (gradle):    builds the Spring Boot fat jar.
#   * Stage 2 (golang):    cross-builds the oj-vsock-client static binary.
#   * Stage 3 (jre+tools): JRE + curl (FC REST), python3 (vsock CONNECT
#                          probe), the fat jar, and the vsock-client.
#
# Note: this image does NOT need docker.io, e2fsprogs, or any of the
# debootstrap toolchain — the rootfs is built ONCE on the host by
# compute.sh.tpl and mounted in.
#
# Build (from repo root):
#   docker buildx build --platform=linux/amd64 \
#     -f sandbox-manager/Dockerfile \
#     -t asia-south1-docker.pkg.dev/online-judge-hk/oj-images/sandbox-manager:latest \
#     --push .
# =============================================================================

# ---- Stage 1: build the fat jar -------------------------------------------
FROM gradle:8.7-jdk17 AS gradle-builder

WORKDIR /build

COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY common/build.gradle             common/
COPY api-gateway/build.gradle        api-gateway/
COPY execution-worker/build.gradle   execution-worker/
COPY sandbox-manager/build.gradle    sandbox-manager/
COPY scoring-pipeline/build.gradle   scoring-pipeline/
COPY leaderboard-service/build.gradle leaderboard-service/
COPY analytics-pipeline/build.gradle analytics-pipeline/
COPY contest-service/build.gradle    contest-service/
COPY problem-service/build.gradle    problem-service/
COPY common/src/main/proto           common/src/main/proto/

RUN gradle :sandbox-manager:dependencies --no-daemon --refresh-dependencies || true

COPY common/src           common/src/
COPY sandbox-manager/src  sandbox-manager/src/

RUN gradle :sandbox-manager:bootJar --no-daemon -x test


# ---- Stage 2: build the host-side vsock-client ----------------------------
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

# curl       — drives the Firecracker REST socket via --unix-socket
# python3    — used for the in-VM vsock CONNECT readiness probe (tiny script,
#              no pip install needed; python3 ships with jammy minimal)
# ca-certs   — for TLS to AR, GCS, etc.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates python3 && \
    rm -rf /var/lib/apt/lists/*

# OpenTelemetry Java agent (Workstream H). Same as execution-worker:
# auto-instruments the JVM on boot, exports OTLP/gRPC when
# OTEL_EXPORTER_OTLP_ENDPOINT is set, no-ops when unset. Custom metrics
# (sandbox.lease.latency_ms, sandbox.pool.ready, sandbox.watchdog.fires_total,
# sandbox.leases.active) are emitted via the agent's bundled SDK from
# com.onlinejudge.sandbox.observability.SandboxMetrics.
RUN curl -fsSL -o /opt/opentelemetry-javaagent.jar \
    https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.10.0/opentelemetry-javaagent.jar

ENV JAVA_TOOL_OPTIONS="-javaagent:/opt/opentelemetry-javaagent.jar"
ENV OTEL_EXPORTER_OTLP_PROTOCOL=grpc
ENV OTEL_LOGS_EXPORTER=none

WORKDIR /app

COPY --from=gradle-builder /build/sandbox-manager/build/libs/sandbox-manager-*.jar /app/app.jar
COPY --from=go-builder     /out/oj-vsock-client                                    /usr/local/bin/oj-vsock-client

ENV JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:InitialRAMPercentage=30 -XX:+ExitOnOutOfMemoryError"

EXPOSE 9100

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
