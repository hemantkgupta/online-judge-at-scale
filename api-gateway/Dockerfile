# =============================================================================
# api-gateway — Spring Boot fat jar in a slim JRE.
#
# Multi-stage build: stage 1 runs Gradle to produce the fat jar, stage 2 is a
# minimal runtime that only needs the jar + a JRE. Build from the repo root
# (so the `common` sibling module is reachable):
#
#   docker buildx build \
#     --platform=linux/amd64 \
#     -f api-gateway/Dockerfile \
#     -t asia-south1-docker.pkg.dev/online-judge-hk/oj-images/api-gateway:latest \
#     --push .
# =============================================================================

# ---- Stage 1: build the fat jar -------------------------------------------
FROM gradle:8.7-jdk17 AS builder

WORKDIR /build

# Copy only the build files first so dep resolution is cached when source changes.
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

# Pre-download deps. If only source code changes below, this layer is cached.
RUN gradle :api-gateway:dependencies --no-daemon --refresh-dependencies || true

# Now copy the actual source and build the fat jar.
COPY common/src      common/src/
COPY api-gateway/src api-gateway/src/

RUN gradle :api-gateway:bootJar --no-daemon -x test


# ---- Stage 2: runtime ------------------------------------------------------
FROM eclipse-temurin:17-jre-jammy

# curl: handy for the actuator health probe; ca-certificates: TLS to AR + IdPs.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the boot jar (its name includes version; rename for predictability).
COPY --from=builder /build/api-gateway/build/libs/api-gateway-*.jar /app/app.jar

# Sensible JVM defaults for a 4 GB control-plane VM. Override via JAVA_OPTS.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:InitialRAMPercentage=40 -XX:+ExitOnOutOfMemoryError"
ENV SERVER_PORT=8088

EXPOSE 8088

# Use sh -c so $JAVA_OPTS expands.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
