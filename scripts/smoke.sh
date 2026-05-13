#!/usr/bin/env bash
#
# scripts/smoke.sh — end-to-end smoke test for the local online-judge stack.
#
# What it validates against the local Kafka + Redis infra:
#   1. Redis is reachable and responds to PING.
#   2. Kafka broker port is reachable (TCP).
#   3. Score-range sharded ZSET writes/reads work (the exact key/format the
#      production scoring pipeline + leaderboard service expect).
#   4. Atomic dual-scope rate limit Lua script behaves correctly under the
#      same INCR + conditional-EXPIRE pattern api-gateway uses.
#   5. The verdict-fallback Redis key contract (verdict:{submissionId} with TTL).
#
# What it does NOT validate:
#   - Running Spring Boot services end-to-end (covered by JUnit integration
#     tests with @EmbeddedKafka — see VerdictPushIntegrationTest).
#   - Real code execution (Docker-in-Docker varies by host).
#   - Kafka produce/consume round-trip (would need kcat or container exec).
#
# Required host tools: redis-cli, nc (netcat). Optional: docker, kcat.
#
# Usage:
#   ./scripts/smoke.sh             # run checks against whatever's on
#                                  # localhost:6379 / localhost:9092
#   ./scripts/smoke.sh --setup     # bring up THIS repo's docker-compose
#                                  # subset (kafka + zookeeper + redis) first
#   REDIS_HOST=... ./scripts/smoke.sh   # point at a different Redis
#
# Exit code 0 on success, non-zero on first failure.

set -euo pipefail

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_REPLICA_HOST="${REDIS_REPLICA_HOST:-localhost}"
REDIS_REPLICA_PORT="${REDIS_REPLICA_PORT:-6380}"
KAFKA_HOST="${KAFKA_HOST:-localhost}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
KAFKA_GLOBAL_HOST="${KAFKA_GLOBAL_HOST:-localhost}"
KAFKA_GLOBAL_PORT="${KAFKA_GLOBAL_PORT:-9093}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { printf "${GREEN}✓${NC} %s\n" "$1"; }
fail() { printf "${RED}✗${NC} %s\n" "$1"; exit 1; }
info() { printf "${YELLOW}…${NC} %s\n" "$1"; }

# ---- Pre-flight ----

HAVE_KCAT=false

check_prereqs() {
    info "Checking prerequisites"
    command -v redis-cli >/dev/null 2>&1 || fail "redis-cli is required (brew install redis)"
    command -v nc        >/dev/null 2>&1 || fail "nc is required"
    if command -v kcat >/dev/null 2>&1; then
        HAVE_KCAT=true
        pass "Prerequisites present (incl. kcat for Kafka round-trip)"
    else
        pass "Prerequisites present (no kcat — Kafka round-trip check will be skipped)"
    fi
}

redis_cli() {
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

replica_cli() {
    redis-cli -h "$REDIS_REPLICA_HOST" -p "$REDIS_REPLICA_PORT" "$@"
}

# ---- Optional setup ----

compose_up() {
    info "Bringing up docker-compose stack (kafka + zookeeper + redis only)"
    cd "$(dirname "$0")/.."
    # CockroachDB and Flink are not required for these smoke checks and their
    # admin UIs collide with common host ports. Users who want the full stack
    # should run `docker compose up -d` themselves.
    docker compose up -d kafka zookeeper redis 2>&1 | tail -5 || true
    info "Waiting up to 30s for Redis to be ready"
    local waited=0
    until redis_cli ping 2>/dev/null | grep -q PONG; do
        sleep 2; waited=$((waited+2))
        [ "$waited" -ge 30 ] && fail "Redis not ready after 30s"
    done
    pass "Stack reachable"
}

# ---- Smoke checks ----

check_redis_alive() {
    info "Redis reachable at ${REDIS_HOST}:${REDIS_PORT}"
    [ "$(redis_cli ping)" = "PONG" ] || fail "Redis PING failed"
    pass "Redis responding"
}

check_kafka_port_open() {
    info "Kafka broker port reachable at ${KAFKA_HOST}:${KAFKA_PORT}"
    nc -z "$KAFKA_HOST" "$KAFKA_PORT" 2>/dev/null \
        || fail "Kafka port ${KAFKA_HOST}:${KAFKA_PORT} not reachable"
    pass "Kafka port open"
}

check_kafka_round_trip() {
    if ! $HAVE_KCAT; then
        info "Skipping Kafka produce/consume round-trip (kcat not installed)"
        return
    fi
    info "Kafka produce/consume round-trip via kcat"
    local topic="smoke-roundtrip-$$"
    local marker="smoke-marker-$(date +%s)"

    # Produce one message (auto-creates the topic if the broker allows it).
    echo "$marker" | kcat -P -b "${KAFKA_HOST}:${KAFKA_PORT}" -t "$topic" -X compression.codec=none 2>/dev/null \
        || fail "kcat produce to '$topic' failed"

    # Consume from the beginning, exit after 1 message or 5s timeout.
    local got
    got=$(kcat -C -b "${KAFKA_HOST}:${KAFKA_PORT}" -t "$topic" \
                -o beginning -c 1 -e -q 2>/dev/null \
            | tail -1)
    [ "$got" = "$marker" ] \
        || fail "Kafka round-trip: expected '$marker', got '$got'"
    pass "Kafka produce → consume successful"
}

check_sharded_zset() {
    info "Score-range sharded ZSET writes/reads"
    local contest="smoke-contest-$$"
    # Bucket 0 (0-29.999M): 1 user
    redis_cli ZADD "leaderboard:${contest}:shard:0" 10000000 "low-user"   >/dev/null
    # Bucket 1 (30-59.999M): 1 user
    redis_cli ZADD "leaderboard:${contest}:shard:1" 45000000 "mid-user"   >/dev/null
    # Bucket 2 (60M+): 2 users
    redis_cli ZADD "leaderboard:${contest}:shard:2" 70000000 "top-user-1" >/dev/null
    redis_cli ZADD "leaderboard:${contest}:shard:2" 65000000 "top-user-2" >/dev/null

    # Top of the highest shard should be top-user-1 (higher score).
    local top
    top=$(redis_cli ZREVRANGE "leaderboard:${contest}:shard:2" 0 0)
    [ "$top" = "top-user-1" ] || fail "top of shard:2 expected top-user-1, got '$top'"

    # Global rank for mid-user: ZCARD(shard:2) + ZREVRANK(shard:1, mid-user) + 1
    local card2 mid_rank
    card2=$(redis_cli ZCARD "leaderboard:${contest}:shard:2")
    mid_rank=$(redis_cli ZREVRANK "leaderboard:${contest}:shard:1" "mid-user")
    local global_rank=$((card2 + mid_rank + 1))
    [ "$global_rank" = "3" ] || fail "mid-user global rank: expected 3, got $global_rank"

    redis_cli DEL "leaderboard:${contest}:shard:0" \
                  "leaderboard:${contest}:shard:1" \
                  "leaderboard:${contest}:shard:2" >/dev/null
    pass "ZSET writes + global rank correct across 3 shards"
}

check_rate_limit_lua() {
    info "Atomic dual-scope rate limit Lua script"
    local user="smoke-user-$$"
    local ip="10.42.42.42"
    local minute=$(($(date +%s) / 60))
    local user_key="rate_limit:user:${user}:${minute}"
    local ip_key="rate_limit:ip:${ip}:${minute}"

    redis_cli DEL "$user_key" "$ip_key" >/dev/null

    # Replicate the production Lua: INCR both buckets, EXPIRE on first hit, return status.
    local script='local u=KEYS[1] local i=KEYS[2] local um=tonumber(ARGV[1]) local im=tonumber(ARGV[2]) local ttl=tonumber(ARGV[3]) local uc=redis.call("INCR",u) if uc==1 then redis.call("EXPIRE",u,ttl) end local ic=redis.call("INCR",i) if ic==1 then redis.call("EXPIRE",i,ttl) end if uc>um then return "USER_LIMIT" end if ic>im then return "IP_LIMIT" end return "OK"'

    for i in 1 2 3; do
        local result
        result=$(redis_cli EVAL "$script" 2 "$user_key" "$ip_key" 3 100 70)
        [ "$result" = "OK" ] || fail "Call $i should be OK, got '$result'"
    done
    # 4th call should trip the user limit (3/min).
    local result
    result=$(redis_cli EVAL "$script" 2 "$user_key" "$ip_key" 3 100 70)
    [ "$result" = "USER_LIMIT" ] || fail "Call 4 should be USER_LIMIT, got '$result'"

    redis_cli DEL "$user_key" "$ip_key" >/dev/null
    pass "Rate limit Lua: 3/min user cap enforced atomically"
}

check_redis_replica_optional() {
    # Opt-in: the leaderboard read-replica from Part 9 is exposed via docker-compose
    # as redis-replica on localhost:6380. If it's running, verify it's actually
    # replicating from the primary and that a write propagates within a short window.
    info "Checking optional Redis read replica at ${REDIS_REPLICA_HOST}:${REDIS_REPLICA_PORT}"
    if ! nc -z "$REDIS_REPLICA_HOST" "$REDIS_REPLICA_PORT" 2>/dev/null; then
        info "Skipping replica check (no Redis listening on ${REDIS_REPLICA_HOST}:${REDIS_REPLICA_PORT})"
        return
    fi

    local role
    role=$(replica_cli INFO replication 2>/dev/null | awk -F: '/^role:/{print $2}' | tr -d '\r\n ')
    if [ "$role" != "slave" ] && [ "$role" != "replica" ]; then
        fail "Expected replica role at ${REDIS_REPLICA_HOST}:${REDIS_REPLICA_PORT}, got '$role'"
    fi

    # Write a marker on the primary, poll the replica for visibility.
    local key="smoke-replica-$$"
    local value="hello-$(date +%s)"
    redis_cli SET "$key" "$value" >/dev/null
    local waited=0
    local got=""
    while [ "$waited" -lt 30 ]; do  # 30 × 0.1s = 3s
        got=$(replica_cli GET "$key" 2>/dev/null)
        [ "$got" = "$value" ] && break
        sleep 0.1
        waited=$((waited+1))
    done
    redis_cli DEL "$key" >/dev/null

    [ "$got" = "$value" ] \
        || fail "Replica did not pick up SET within 3s — replication broken?"
    pass "Replica replicating from primary (write propagated in $((waited * 100))ms)"
}

check_changefeed_optional() {
    # The CockroachDB changefeed is opt-in (see database/changefeed-setup.sql).
    # If a CockroachDB container is running, check whether a changefeed job
    # exists; surface it as INFO so users know the production CDC path is live.
    info "Checking whether a CockroachDB changefeed is active (optional)"
    if ! command -v docker >/dev/null 2>&1; then
        info "Skipping changefeed check (docker not available)"
        return
    fi
    local crdb_ctr
    crdb_ctr=$(docker ps --filter "ancestor=cockroachdb/cockroach:v24.1.10" --format '{{.ID}}' 2>/dev/null | head -1)
    if [ -z "$crdb_ctr" ]; then
        info "Skipping changefeed check (cockroachdb v24.1.x container not running — see database/changefeed-setup.sql)"
        return
    fi
    local jobs
    jobs=$(docker exec "$crdb_ctr" cockroach sql --insecure --format=csv \
            -e "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status IN ('running','paused');" 2>/dev/null \
            | tail -1 | tr -d ' \r')
    if [ -n "$jobs" ] && [ "$jobs" != "count" ] && [ "$jobs" -gt 0 ]; then
        pass "CockroachDB changefeed active ($jobs job(s)) — running real CDC, not the polling publisher"
    else
        info "No CockroachDB changefeed active — using the polling publisher (default). Run database/changefeed-setup.sql to enable CDC."
    fi
}

check_mirrormaker2_optional() {
    # Opt-in: docker-compose ships a second Kafka broker (`kafka-global`,
    # localhost:9093) and a `mirrormaker2` service replicating regional →
    # global. If both are reachable, verify a message produced to
    # `evaluated_results` on the regional broker shows up on the global
    # broker as `regional.evaluated_results` (MM2 DefaultReplicationPolicy).
    info "Checking optional MirrorMaker 2 replication ${KAFKA_HOST}:${KAFKA_PORT} → ${KAFKA_GLOBAL_HOST}:${KAFKA_GLOBAL_PORT}"
    if ! nc -z "$KAFKA_GLOBAL_HOST" "$KAFKA_GLOBAL_PORT" 2>/dev/null; then
        info "Skipping MM2 check (no global Kafka listening on ${KAFKA_GLOBAL_HOST}:${KAFKA_GLOBAL_PORT})"
        return
    fi
    if ! $HAVE_KCAT; then
        info "Skipping MM2 round-trip (kcat not installed)"
        return
    fi

    local marker="mm2-marker-$(date +%s)-$$"
    # Produce to the regional topic the blog uses for verdicts.
    echo "$marker" | kcat -P -b "${KAFKA_HOST}:${KAFKA_PORT}" -t "evaluated_results" \
            -X compression.codec=none 2>/dev/null \
        || fail "kcat produce to regional 'evaluated_results' failed"

    # Poll the global broker for the mirrored topic. MM2's refresh interval is
    # 5s in our config; give it up to 30s on first run (topic auto-creation +
    # connector start-up dominates).
    local waited=0
    local got=""
    while [ "$waited" -lt 60 ]; do  # 60 × 0.5s = 30s
        got=$(kcat -C -b "${KAFKA_GLOBAL_HOST}:${KAFKA_GLOBAL_PORT}" \
                    -t "regional.evaluated_results" \
                    -o beginning -c 1 -e -q 2>/dev/null \
              | grep -F "$marker" | tail -1)
        [ -n "$got" ] && break
        sleep 0.5
        waited=$((waited+1))
    done

    if [ -n "$got" ]; then
        pass "MM2 replicated regional → global within $((waited * 500))ms (topic: regional.evaluated_results)"
    else
        info "MM2 reachable but no replicated message observed within 30s — connector may still be warming up. Re-run after the mirrormaker2 service stabilises."
    fi
}

check_verdict_fallback_key() {
    info "Verdict-fallback Redis key contract (verdict:{submissionId} with TTL)"
    local sid="smoke-sub-$$"
    local payload='{"submissionId":"'"$sid"'","userId":"u","result":"ACCEPTED","executionTimeMs":42}'

    # Match the VerdictPushConsumer's behavior: set with a 24-hour TTL.
    redis_cli SET "verdict:${sid}" "$payload" EX 86400 >/dev/null

    local got ttl
    got=$(redis_cli GET "verdict:${sid}")
    ttl=$(redis_cli TTL "verdict:${sid}")

    [ "$got" = "$payload" ] || fail "Verdict payload mismatch"
    [ "$ttl" -gt 0 ] || fail "Verdict TTL not set"

    redis_cli DEL "verdict:${sid}" >/dev/null
    pass "Verdict key stores payload with positive TTL"
}

# ---- Main ----

main() {
    local setup=false teardown=false
    while [ $# -gt 0 ]; do
        case "$1" in
            --setup)    setup=true; shift ;;
            --teardown) teardown=true; shift ;;
            -h|--help)
                grep '^#' "$0" | head -30
                exit 0
                ;;
            *) fail "Unknown flag: $1" ;;
        esac
    done

    check_prereqs
    if $setup; then compose_up; fi

    check_redis_alive
    check_kafka_port_open
    check_kafka_round_trip
    check_sharded_zset
    check_rate_limit_lua
    check_verdict_fallback_key
    check_redis_replica_optional
    check_changefeed_optional
    check_mirrormaker2_optional

    printf "\n${GREEN}All smoke checks passed.${NC}\n"

    if $teardown; then
        info "Tearing down (kafka + zookeeper + redis only)"
        cd "$(dirname "$0")/.."
        docker compose stop kafka zookeeper redis 2>&1 | tail -3 || true
    fi
}

main "$@"
