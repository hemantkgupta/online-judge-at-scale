# Online Judge at Scale — Companion Code

A working local implementation of the system designed in **"Designing a Global Online Judge at Scale: The Complete Engineering Guide"** ([blog repo](https://github.com/hemantkgupta/CSE-Raw)).

The codebase is an 8-module Gradle build (Java 17, Spring Boot 3.2, Apache Flink 1.18, CockroachDB, Kafka, Redis, ClickHouse, SigNoz) that runs end-to-end on `docker-compose`.

## Quick orientation

| What | Where |
|---|---|
| **Blog ↔ code map** (every blog part → source files) | [`docs/code-companion.md`](docs/code-companion.md) |
| **What's implemented locally vs production gaps** | [`docs/code-companion.md` — Gaps section](docs/code-companion.md#gaps-blog-claims-not-yet-implemented) |
| **Original implementation plan** (now mostly executed) | [`docs/implementation-plan.md`](docs/implementation-plan.md) |
| **Design rationale, foundational concepts** | [`docs/research-checkpoint.md`](docs/research-checkpoint.md) |
| **Historical: parity-with-KV-store plan** | [`docs/parity-plan.md`](docs/parity-plan.md) |

## Modules

| Module | Role | Blog reference |
|---|---|---|
| `api-gateway` | HTTP entry point, rate limit, transactional outbox | Parts 3 and 6 |
| `contest-service` | 5-state lifecycle FSM, AES-256-GCM bundle encryption, T₀/T₁ automation | Part 4 |
| `problem-service` | Problem CRUD, pre-signed R2 URLs, pretest/system-test ordinal split | Part 5 |
| `execution-worker` | Kafka consumers (Phase 1 + Phase 2), Docker sandbox, idempotency guard | Part 7 |
| `scoring-pipeline` | Flink job: stateful scoring, atomic Lua sink to score-range-sharded Redis | Part 8 |
| `leaderboard-service` | Sharded ZSET reads, leaderboard-delta + verdict push over WebSocket STOMP | Part 9 |
| `analytics-pipeline` | ClickHouse batch ingest from Kafka | Part 8 (sink) |
| `common` | Protobuf event schemas, shared types (`ScoreRangeShardRouter`) | Part 2 |

## Run locally

```bash
# Bring up infrastructure (CockroachDB, Kafka, Redis, ClickHouse, Flink, SigNoz)
docker compose up -d

# Build everything
./gradlew build

# Run the test suite (uses embedded Kafka — no Docker required for tests)
./gradlew test

# Start a Spring Boot service (example: api-gateway)
./gradlew :api-gateway:bootRun
```

The smoke-test suite (in particular `leaderboard-service`'s `VerdictPushIntegrationTest`) exercises a real Spring context + embedded Kafka + STOMP WebSocket subscriber, so a green `./gradlew test` confirms the verdict-push wiring works end-to-end without Docker.

## What's intentionally not implemented locally

The blog calls out a number of production-grade mechanisms that don't run inside `docker-compose`. The complete list lives in two places:

- The repo's [Gaps section](docs/code-companion.md#gaps-blog-claims-not-yet-implemented) (file-level perspective)
- The blog's **Gaps vs Production** table at the end of `online-judge-at-scale.md` (architectural perspective)

Highlights: multi-region Redis read replicas (single node demo of the read/write split), multi-instance Push Service runtime (the co-partitioning *logic* — `PartitionAssigner`, `VerdictConnectionRegistry`, STOMP CONNECT/DISCONNECT interceptor, and registry-gated push — is wired up; a single instance just holds partition #0 of every user), CockroachDB REGIONAL BY ROW (the `region` column, `RegionResolver`, and `database/multi-region-setup.sql` are in place — only the multi-node cluster is missing). Kafka cross-cluster replication is implemented locally with **MirrorMaker 2** (open-source, Apache 2.0) standing in for the blog's Confluent Cluster Linking — `docker-compose` boots a regional and a global broker plus an MM2 worker replicating between them. The sandbox runtime is selectable: Docker (default, cross-platform, optionally with Seccomp-BPF + capability drop + private cgroup namespace on Linux) or Firecracker microVMs (Linux + `/dev/kvm`, set `app.sandbox.backend=firecracker` — operator notes in `infra/firecracker/README.md`); the Firecracker backend is **hardware-validated on a Raspberry Pi 5** (full 267-test suite green; see `infra/firecracker/raspberry-pi-setup.md`). JWT authentication is wired through Spring Security + JJWT; every cross-service Kafka topic now carries Protobuf bytes (schemas in `common/src/main/proto/events.proto`), with the WebSocket leg to the browser remaining JSON. CockroachDB changefeed CDC is supported as an opt-in flag alongside the default polling outbox publisher. The local repo exercises the architectural shapes — sharded ZSET keys, transactional outbox with regional locality, proto wire format end-to-end, idempotent consumers, Phase 1 → Phase 2 promotion, atomic dual-scope rate limit, per-user WebSocket verdict push with HTTP fallback — even where the runtime is simplified.
