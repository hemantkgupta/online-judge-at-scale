# =============================================================================
# problem-service — Spring Boot fat jar in a slim JRE.
#
# Mirrors the api-gateway multi-stage shape. Build from the repo root so the
# `common` sibling module is reachable:
#
#   docker buildx build \
#     --platform=linux/amd64 \
#     -f problem-service/Dockerfile \
#     -t asia-south1-docker.pkg.dev/online-judge-hk/oj-images/problem-service:latest \
#     --push .
# =============================================================================

# ---- Stage 1: build the fat jar -------------------------------------------
FROM gradle:8.7-jdk17 AS builder

WORKDIR /build

# Copy only the build files first so dep resolution is cached when source changes.
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY common/build.gradle              common/
COPY api-gateway/build.gradle         api-gateway/
COPY execution-worker/build.gradle    execution-worker/
COPY scoring-pipeline/build.gradle    scoring-pipeline/
COPY leaderboard-service/build.gradle leaderboard-service/
COPY analytics-pipeline/build.gradle  analytics-pipeline/
COPY contest-service/build.gradle     contest-service/
COPY problem-service/build.gradle     problem-service/
COPY common/src/main/proto            common/src/main/proto/

RUN gradle :problem-service:dependencies --no-daemon --refresh-dependencies || true

COPY common/src          common/src/
COPY problem-service/src problem-service/src/

RUN gradle :problem-service:bootJar --no-daemon -x test


# ---- Stage 2: runtime ------------------------------------------------------
FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/problem-service/build/libs/problem-service-*.jar /app/app.jar

ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:InitialRAMPercentage=40 -XX:+ExitOnOutOfMemoryError"
ENV SERVER_PORT=8089

EXPOSE 8089

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
