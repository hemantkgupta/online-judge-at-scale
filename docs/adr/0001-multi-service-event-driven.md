# ADR-0001: Multi-Service Event-Driven Architecture with Kafka Spine

**Status**: Accepted  
**Date**: 2026-04-24  
**Deciders**: Engineering team  

## Context

We are building a global online judge that must accept untrusted code from 1M concurrent users, execute it safely, score it fairly with exactly-once semantics, and serve leaderboard rankings at sub-10ms latency — all while absorbing a 5-10x traffic spike in the final minutes of a 90-minute contest.

The core tension: the write path must accept submissions in under 100ms, the compute path takes 1-30 seconds per submission, and the read path must serve millions of reads per second at under 10ms. These three workloads cannot share infrastructure without destroying each other.

## Decision

**10-service event-driven architecture with Apache Kafka as the messaging spine.**

Services: API Gateway, Problem Service, Contest Service, Submission Service, Sandbox Manager, Execution Service, Verdict Pipeline (Flink), Leaderboard Service, Push/Notification Service, Analytics Service. Each service owns its data; no service reads another service's database. State propagates through Kafka topics and a small set of explicit RPC contracts.

---

## Alternatives Considered

### 1. Monolith Judge

A single service handles submission ingest, code execution, scoring, and leaderboard queries.

**Why rejected:**
- Code execution (1-30s, CPU-bound) and submission ingest (sub-100ms, I/O-bound) cannot share a thread pool without the compute path starving the ingest path during the surge.
- A single deployment unit means a bug in scoring takes down submission ingest. Blast radius is the entire system.
- Scaling requires scaling everything together — you cannot add execution capacity without also scaling the leaderboard read path.
- At 14,000 submissions/sec peak, a monolith's internal queue becomes the single bottleneck with no backpressure isolation.

### 2. Serverless (Lambda per Submission)

Each submission triggers a Lambda function that executes the code and writes the verdict.

**Why rejected:**
- Lambda has a 15-minute timeout but provides no hardware isolation for adversarial code. Containers share the host kernel — the same security defect as Docker (see CVE-2024-28185, CVE-2024-29021 against Judge0).
- Cold starts add 1-5s latency, unacceptable for the pretest verdict SLA (sub-2s end-to-end).
- Stateful scoring (per-user score across problems) requires an external database call per verdict. At 14,000 verdicts/sec, this becomes the bottleneck. Flink's managed local state eliminates this.
- No pull-based backpressure. Lambda scales by invocation count, not by available capacity — during the surge, it can over-provision beyond the sandbox fleet's capacity and fail at the execution layer.
- Cost: 14,000 concurrent invocations x 30s average = 420,000 GB-seconds per minute at peak. Kafka consumers on reserved instances are an order of magnitude cheaper.

### 3. Single-Queue RPC (Request/Reply)

One Kafka topic, one consumer group, synchronous RPC from gateway to execution service, reply queue for verdicts.

**Why rejected:**
- Synchronous RPC couples ingestion throughput to execution throughput. If execution falls behind, the gateway blocks — exactly what the 202 Accepted contract is designed to prevent.
- A single topic cannot express priority between pretest (fast, live) and system test (slow, deferrable) submissions. Separate topics with separate consumer groups give clean priority isolation.
- No event-time scoring. RPC-style architectures use request timestamps, which carry execution-time skew (a Python solution's verdict arrives 3s later than a C++ solution's). The gateway T0 timestamp propagated through Kafka events is the only fair basis for penalty calculation.

---

## Why Kafka over RabbitMQ for the Submission Pipeline

| Property | Kafka | RabbitMQ |
|---|---|---|
| **Consumer model** | Pull-based: workers poll when they have capacity | Push-based: broker pushes messages to consumers |
| **Backpressure** | Natural: busy workers stop polling, messages stay in the topic | Requires prefetch tuning; under surge, the broker's push rate overwhelms consumers |
| **Partition ordering** | Per-partition FIFO: key by `user_id` guarantees per-user ordering | Per-queue FIFO only; routing by user requires per-user queues or consistent hash exchange |
| **Replay** | Retained for configurable duration; consumers rewind on failure | Messages deleted after acknowledgment; no replay without DLQ + re-publish |
| **Throughput at scale** | 14,000+ messages/sec sustained per partition, millions aggregate | Degrades under high queue depth (memory-backed broker) |
| **CDC integration** | CockroachDB changefeed publishes directly to Kafka topics | Requires an intermediary (Debezium → Kafka → RabbitMQ shovel) |

**The deciding factor is pull-based backpressure.** During the final-minute surge, submission rate spikes to 14,000/sec while the execution fleet processes at 5,000-10,000/sec. With Kafka, the gap accumulates in the topic as consumer lag — workers continue pulling at their own pace, and the backlog drains after the surge. With RabbitMQ, the broker pushes messages faster than consumers can process them; without careful prefetch tuning, consumers OOM or reject messages, triggering redelivery storms.

Kafka's partition ordering by `user_id` is also structurally necessary: it guarantees that all submissions from the same user are processed in order by the execution tier and scored in order by Flink. RabbitMQ achieves this only with a consistent hash exchange, which adds operational complexity and doesn't compose with Kafka's native integration with Flink and CockroachDB changefeeds.

---

## Why CockroachDB over PostgreSQL

| Property | CockroachDB | PostgreSQL |
|---|---|---|
| **Multi-region locality** | `REGIONAL BY ROW` pins writes to the user's region (5-10ms). `GLOBAL` gives sub-10ms reads from every region. Per-table locality policies. | Single-region primary with read replicas. Cross-region writes require replication lag or synchronous replication with high latency. |
| **Native CDC** | Built-in changefeeds emit row-level events directly to Kafka. No Debezium, no Kafka Connect. | Requires Debezium + logical replication slots + Kafka Connect cluster. |
| **UUID key distribution** | Designed for UUID primary keys; automatic range-splitting distributes writes across nodes. | B-tree indexes on UUIDs cause random I/O and page splits under write-heavy load. |
| **Raft consensus** | Serializable by default. Each range is a Raft group; one node failure is tolerated transparently. | Synchronous replication for strong consistency, or async with potential data loss on failover. |

**The deciding factor is `REGIONAL BY ROW` vs `GLOBAL` on the same cluster.** The submissions table is write-heavy/read-rare (written once, read once by the same-region execution worker) — `REGIONAL BY ROW` keeps writes fast by replicating only within the region. The contests table is read-heavy/write-rare (status checked thousands of times per second per region, changed twice per contest) — `GLOBAL` gives sub-10ms reads from every region. Two tables on the same cluster, two different locality policies, each matching its workload. PostgreSQL has no equivalent — you'd need a separate replication topology per table.

---

## Why Firecracker over Docker in Production

| Property | Firecracker MicroVM | Docker Container |
|---|---|---|
| **Isolation boundary** | Hardware-assisted KVM: each submission gets its own guest kernel. Attacker must escape the hypervisor. | Shared host kernel: namespaces + cgroups enforced by the kernel on itself. Kernel vulnerability = host compromise. |
| **Track record for adversarial code** | Used by AWS Lambda, Fly.io for multi-tenant untrusted workloads. No known escape CVEs. | Judge0 CVE-2024-28185 (CVSS 10.0): container escape via symlink race. CVE-2024-29021 (CVSS 9.1): SSRF through sandbox. |
| **Boot time** | ~125ms (minimal guest kernel, pre-built rootfs) | ~50ms (process start in existing kernel) |
| **Memory overhead** | ~5-10MB per VM (KVM + minimal guest kernel) | ~1-2MB per container (namespace metadata) |
| **Timing accuracy** | Dedicated vCPU, no kernel scheduler contention with other VMs | Shared kernel scheduler; other containers' syscalls cause context switches that skew timing |

**The deciding factor is the threat model.** For an online judge receiving code from anonymous, adversarial users at 14,000/sec, Docker's shared-kernel architecture is a design defect, not a risk to mitigate. The 75ms boot time penalty of Firecracker is absorbed by the warm pool (VMs are pre-booted before they're needed). The memory overhead scales linearly but is modest — 21,000 concurrent VMs x 10MB = 210GB of overhead, spread across 200-400 hosts.

**Local development gap:** Firecracker requires KVM (Linux + bare metal or KVM-enabled instances). macOS does not support KVM. The local implementation uses Docker containers with `--network none`, `--memory`, `--cpus`, `--pids-limit`, and `--read-only` — same resource constraints, weaker security boundary. This is documented and acceptable for trusted local development.

---

## Why Flink over Kafka Streams for Scoring

| Property | Apache Flink | Kafka Streams |
|---|---|---|
| **Managed state** | `ValueState`, `MapState` backed by RocksDB with incremental checkpointing to S3. State is local to the task, never hits an external DB on the hot path. | In-memory or RocksDB-backed state stores. Checkpoint via Kafka changelog topics (not S3). |
| **Event-time processing** | First-class: `WatermarkStrategy`, `BoundedOutOfOrderness`, side outputs for late data. Gateway T0 timestamp used as the authoritative event time for fair scoring. | Supported but less ergonomic. Watermark management requires manual `TimestampExtractor` and punctuation. |
| **ABS checkpointing** | Asynchronous Barrier Snapshotting: globally consistent snapshots without stopping the pipeline. On failure, restore state + rewind Kafka to the snapshot's offset. Exactly-once across state and output. | Relies on Kafka transactions for exactly-once. Requires transactional producers and consumers — coupling the exactly-once guarantee to Kafka's transactional protocol. |
| **Deployment model** | Standalone cluster (JobManager + TaskManagers). Fat JAR submitted to the cluster. Resource isolation from the Kafka brokers. | Embedded in the application (library, not a cluster). Scales by adding application instances. Shares JVM with application logic. |
| **Late data handling** | Side outputs: late events (beyond watermark) are routed to a separate stream for reconciliation. | No native side output. Late events are either dropped or require manual windowing workarounds. |

**The deciding factor is managed state with ABS checkpointing.** At 14,000 verdicts/sec, the scoring function must maintain per-user state (solved problems, penalty minutes, wrong attempts) for 1M users. Flink's `KeyedProcessFunction` with `ValueState<ScoringState>` keeps this state local to the task, checkpointed incrementally to S3 every 30 seconds. On failure, Flink restores state and rewinds Kafka — the scoring function contains zero recovery logic. Kafka Streams achieves similar state locality but ties its exactly-once guarantee to Kafka's transactional protocol, which couples the scoring pipeline's correctness to Kafka broker transaction coordinator availability.

The event-time watermark with `BoundedOutOfOrderness(5 min)` is also structurally necessary: it keeps the scoring window open after contest close to absorb late-arriving verdicts from VMs that were executing at close time. Flink's side output routes genuinely late events to a reconciliation job. Kafka Streams has no clean equivalent.

---

## Consequences

**Positive:**
- Three workloads (ingest, compute, read) scale independently. Tokyo's surge cannot back up Frankfurt.
- Every downstream store is derived from Kafka — state can be rebuilt by replay.
- Exactly-once scoring via Flink ABS without external distributed transactions.
- Firecracker eliminates the shared-kernel attack surface that has produced every major judge CVE.

**Negative:**
- Operational complexity: 6 infrastructure systems (Kafka, CockroachDB, Redis, Flink, ClickHouse, Firecracker) to operate.
- Local development requires Docker as a Firecracker substitute (weaker security boundary, documented gap).
- Cluster Linking adds sub-second latency to the scoring path (acceptable for eventually-consistent leaderboard).
- CockroachDB changefeeds are a CockroachDB-specific feature — migration to a different database would require adopting Debezium.

## References

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [Firecracker: Lightweight Virtualization for Serverless Applications](https://www.usenix.org/conference/nsdi20/presentation/agache)
- [Flink Asynchronous Barrier Snapshotting](https://arxiv.org/abs/1506.08603)
- [CockroachDB Multi-Region Capabilities](https://www.cockroachlabs.com/docs/stable/multiregion-overview)
- [Judge0 CVE-2024-28185](https://nvd.nist.gov/vuln/detail/CVE-2024-28185)
