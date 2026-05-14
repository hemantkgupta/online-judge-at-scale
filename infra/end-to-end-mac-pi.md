# End-to-end Mac ↔ Raspberry Pi pipeline

Run the full submission flow with the **regional services on your Mac** (via
`docker-compose`) and the **`execution-worker` on a Raspberry Pi 5** with
`APP_SANDBOX_BACKEND=firecracker`. A submission posted through the gateway on
the Mac executes inside a real Firecracker microVM on the Pi and the verdict
travels back through Kafka to the Mac. Same code paths as production — only
the network topology is collapsed onto two boxes.

## Topology

```
   Mac (macOS, Apple Silicon)                       Raspberry Pi 5 (Pi OS Bookworm, KVM)
   ──────────────────────────                       ──────────────────────────
                                                    ./gradlew :execution-worker:bootRun
   docker-compose:                                    APP_SANDBOX_BACKEND=firecracker
     • zookeeper                                      SPRING_KAFKA_BOOTSTRAP_SERVERS=<mac-ip>:9092
     • kafka            (advertised on Mac LAN IP)    SPRING_DATASOURCE_URL=jdbc:postgresql://<mac-ip>:26257/...
     • cockroachdb      :26257 + :8080 (UI)
     • redis            :6379                             │   consumes submissions.pretest (proto)
                                                          │   spins up Firecracker microVM
   ./gradlew :api-gateway:bootRun                         │   publishes evaluated_results (proto)
   SERVER_PORT=8088                                       │
     • POST /api/v1/auth/token                            ▼
     • POST /api/v1/submissions    ◀───── LAN  9092 ─────┘
     • GET  /api/v1/submissions/:id/verdict
```

Everything that isn't `execution-worker` stays on the Mac. The worker is the
only thing that needs KVM — and the Pi is the only host that has it.

## Pre-requisites

- **Mac**: Java 17 + Gradle (you already have these), Docker Desktop / OrbStack
  with the repo's `docker-compose.yml`.
- **Pi 5**: Firecracker + kernel + rootfs installed via
  [`setup-on-pi.sh`](firecracker/setup-on-pi.sh) — done as part of Path A.
- **Network**: Mac and Pi on the same LAN, with Pi able to TCP-connect to the
  Mac on ports 9092, 26257.

## Step 0 — Find your Mac's LAN IP

On the Mac:

```sh
ipconfig getifaddr en0     # Wi-Fi; try en1 if you're on wired Ethernet
```

That's your `<mac-ip>`. Note it; you'll use it on both sides.

## Step 1 — Bring up the Mac infrastructure

The advertised-listener change in `docker-compose.yml` is driven by env var
`KAFKA_HOST_EXTERNAL`. Set it to your Mac LAN IP so the Pi can reach Kafka.

```sh
cd ~/code-all/online-judge-at-scale
export KAFKA_HOST_EXTERNAL=<mac-ip>
docker compose up -d zookeeper kafka cockroachdb redis
```

Wait ~30s for everything to start, then sanity-check from the Mac:

```sh
docker compose ps
nc -z <mac-ip> 9092 && echo 'kafka OK'
nc -z <mac-ip> 26257 && echo 'cockroachdb OK'
nc -z <mac-ip> 6379 && echo 'redis OK'
```

From the Pi, do the same (this is what actually matters):

```sh
nc -z <mac-ip> 9092 && echo 'kafka reachable from Pi'
nc -z <mac-ip> 26257 && echo 'cockroach reachable from Pi'
```

If anything fails, check your Mac firewall: **System Settings → Network →
Firewall → Options → Docker.app should be allowed for incoming connections**.

## Step 2 — Run the API gateway on the Mac

CockroachDB's admin UI grabs port 8080, which collides with Spring Boot's
default. Override the gateway to port 8088:

```sh
cd ~/code-all/online-judge-at-scale
SERVER_PORT=8088 ./gradlew :api-gateway:bootRun
```

First boot runs the Flyway migrations against CockroachDB (creates
`submissions`, `outbox_events`, etc.). Watch for `Started ApiGatewayApplication`.

Health-check from another Mac terminal:

```sh
curl http://localhost:8088/api/v1/submissions/health
# → OK
```

## Step 3 — Run the execution-worker on the Pi

SSH into the Pi (`ssh hemant@raspberrypi.local`). Then:

```sh
cd ~/online-judge-at-scale
export MAC_IP=<mac-ip>

APP_SANDBOX_BACKEND=firecracker \
  APP_SANDBOX_FIRECRACKER_API_SOCK_DIR=/tmp \
  APP_SANDBOX_FIRECRACKER_KERNEL_IMAGE=/var/lib/firecracker/vmlinux \
  APP_SANDBOX_FIRECRACKER_ROOTFS_IMAGE=/var/lib/firecracker/rootfs.ext4 \
  SPRING_KAFKA_BOOTSTRAP_SERVERS=${MAC_IP}:9092 \
  SPRING_DATASOURCE_URL="jdbc:postgresql://${MAC_IP}:26257/defaultdb?sslmode=disable" \
  SPRING_DATASOURCE_USERNAME=root \
  ./gradlew :execution-worker:bootRun
```

Watch for these lines in the Pi log:

```
[firecracker] backend selected (Linux + /dev/kvm present)
Started ExecutionWorkerApplication ... seconds
o.a.k.c.c.internals.ConsumerCoordinator  : ... Subscribed to topic(s): submissions.pretest
o.a.k.c.c.internals.ConsumerCoordinator  : ... Subscribed to topic(s): submissions.system
```

If you see those, the Pi's worker is **registered as a consumer on your Mac's
Kafka**, with the Firecracker backend selected.

## Step 4 — Submit a test request

From a third terminal on the Mac:

```sh
# Mint a JWT for a synthetic user-id
TOKEN=$(curl -sS -X POST http://localhost:8088/api/v1/auth/token \
          -H 'Content-Type: application/json' \
          -d '{"userId":"00000000-0000-0000-0000-000000000001"}' \
        | python3 -c 'import sys,json; print(json.load(sys.stdin)["accessToken"])')

# Submit
SUB_RESPONSE=$(curl -sS -X POST http://localhost:8088/api/v1/submissions \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
        "problemId":"11111111-1111-1111-1111-111111111111",
        "contestId":"22222222-2222-2222-2222-222222222222",
        "language":"python",
        "code":"print(42)"
      }')
echo "$SUB_RESPONSE"

SUB_ID=$(echo "$SUB_RESPONSE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["submissionId"])')
echo "Submission ID: $SUB_ID"
```

What you should see, in order:

1. **API gateway log (Mac)** — `[gateway] Accepted submission=<id> user=00000000-…`
2. **`OutboxPublisherJob` log (Mac)** — `[outbox] Published submission=<id> to topic=submissions.pretest`
3. **Execution-worker log (Pi)** — `[worker:pretest] Received submission=<id> user=… lang=python region=us-east-1`
4. **Execution-worker log (Pi)** — after ~5 s, `[firecracker] TLE submission=<id> time=≈5000ms`
5. **Execution-worker log (Pi)** — `[worker:pretest] Verdict submission=<id> result=TIME_LIMIT_EXCEEDED time=5xxxms`

The `TIME_LIMIT_EXCEEDED` is expected with the Firecracker CI's stock rootfs
(no `/init` harness to run the contestant code). A production rootfs would
return `ACCEPTED` or whatever the test harness produces — but the *path* is
identical: Mac gateway → Mac Kafka → Pi worker → real microVM boot → Pi worker
→ Mac Kafka.

## Step 5 — Verify the verdict round-tripped to the Mac

Three ways, pick whichever:

### a. `kcat` against `evaluated_results` (fastest)

```sh
brew install kcat   # if not installed
kcat -C -b localhost:9092 -t evaluated_results -o end -c 1 -e 2>/dev/null \
  | head -c 200
```

You'll see ~200 bytes of binary protobuf — the `VerdictEvent`. Inspect with:

```sh
kcat -C -b localhost:9092 -t evaluated_results -o end -c 1 -e 2>/dev/null \
  | protoc --decode=com.onlinejudge.events.VerdictEvent \
           --proto_path=common/src/main/proto common/src/main/proto/events.proto
```

(Needs `protoc` on the Mac — `brew install protobuf`.)

### b. HTTP verdict-fallback endpoint

```sh
# Wait a moment for the leaderboard-service to cache the verdict in Redis.
# Then:
curl -sS -H "Authorization: Bearer $TOKEN" \
  http://localhost:8088/api/v1/submissions/$SUB_ID/verdict
```

**Note:** this requires `leaderboard-service` running on the Mac (it's the
component that consumes `evaluated_results` and caches to Redis at
`verdict:{id}`). To exercise this path, also run:

```sh
./gradlew :leaderboard-service:bootRun
```

### c. Full pipeline (Flink scoring → WebSocket push)

To see the verdict trigger a leaderboard update + WebSocket push, also run
`./gradlew :scoring-pipeline:bootRun` and connect a STOMP client to
`ws://localhost:8082/ws` subscribed to `/topic/verdicts/00000000-0000-0000-0000-000000000001`.

But for the core "Mac↔Pi proves the integration" demo, option (a) is enough.

## Troubleshooting

**Pi can't reach Mac on 9092.** Mac firewall is blocking. System Settings →
Network → Firewall → Options → ensure Docker/OrbStack has incoming connections
allowed.

**Pi's Kafka consumer gets metadata but then tries to connect to `localhost:9092`.**
You forgot `KAFKA_HOST_EXTERNAL=<mac-ip>` when bringing up docker-compose, so
Kafka is still advertising `localhost:9092` to all clients. Tear down and
restart kafka:
```sh
KAFKA_HOST_EXTERNAL=<mac-ip> docker compose up -d --force-recreate kafka
```

**`Connection refused` on `26257`.** CockroachDB is bound to `0.0.0.0:26257`
in the compose file but Docker on macOS sometimes needs you to allow the port
through the firewall explicitly. Confirm from the Mac with `nc -z <mac-ip>
26257` first; if that fails from the Mac itself, the issue is firewall, not
Pi-side.

**Worker on Pi: `IllegalStateException: /dev/kvm is not available`.** Either
you're not on a KVM-enabled kernel (verify with `ls -la /dev/kvm`) or the
worker's user isn't in the `kvm` group (`groups | grep kvm`). If you re-flashed
the Pi recently, you need a fresh login for group membership to apply.
