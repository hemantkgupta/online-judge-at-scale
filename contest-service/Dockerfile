# =============================================================================
# contest-service — Spring Boot fat jar in a slim JRE.
#
# Mirrors the problem-service multi-stage shape. Build from the repo root so
# the `common` sibling module is reachable:
#
#   docker buildx build \
#     --platform=linux/amd64 \
#     -f contest-service/Dockerfile \
#     -t asia-south1-docker.pkg.dev/online-judge-hk/oj-images/contest-service:latest \
#     --push .
#
# Roadmap §4.19: contest-service owns the contest lifecycle (start/end
# windows, freeze, encryption bundle URLs). Today it ships only the
# REST + JPA layer; the freeze-window enforcement runs in api-gateway via
# ContestWindowFilter (gated by env that's currently unset — see roadmap).
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

RUN gradle :contest-service:dependencies --no-daemon --refresh-dependencies || true

COPY common/src          common/src/
COPY contest-service/src contest-service/src/

RUN gradle :contest-service:bootJar --no-daemon -x test


# ---- Stage 2: runtime ------------------------------------------------------
FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/contest-service/build/libs/contest-service-*.jar /app/app.jar

# MaxRAMPercentage is ignored if JAVA_TOOL_OPTIONS is set in compose (the
# control-plane VM caps each service heap individually via that env to
# leave room for kafka + cockroachdb + the other JVMs).
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:InitialRAMPercentage=40 -XX:+ExitOnOutOfMemoryError"
ENV SERVER_PORT=8084

EXPOSE 8084

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
