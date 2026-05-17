# =============================================================================
# leaderboard-service — Spring Boot fat jar in a slim JRE.
#
# Mirrors the problem-service multi-stage shape. Build from the repo root so
# the `common` sibling module is reachable:
#
#   docker buildx build \
#     --platform=linux/amd64 \
#     -f leaderboard-service/Dockerfile \
#     -t asia-south1-docker.pkg.dev/online-judge-hk/oj-images/leaderboard-service:latest \
#     --push .
#
# Roadmap §4.22: leaderboard-service consumes evaluated_results, maintains
# a Redis sorted-set ranking, and serves the WebSocket /ws/leaderboard
# endpoint that the future React SPA subscribes to. No CRDB dependency —
# Redis is the only stateful backend.
# =============================================================================

# ---- Stage 1: build the fat jar -------------------------------------------
FROM gradle:8.7-jdk17 AS builder

WORKDIR /build

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

RUN gradle :leaderboard-service:dependencies --no-daemon --refresh-dependencies || true

COPY common/src              common/src/
COPY leaderboard-service/src leaderboard-service/src/

RUN gradle :leaderboard-service:bootJar --no-daemon -x test


# ---- Stage 2: runtime ------------------------------------------------------
FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/leaderboard-service/build/libs/leaderboard-service-*.jar /app/app.jar

ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:InitialRAMPercentage=40 -XX:+ExitOnOutOfMemoryError"
ENV SERVER_PORT=8082

EXPOSE 8082

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
