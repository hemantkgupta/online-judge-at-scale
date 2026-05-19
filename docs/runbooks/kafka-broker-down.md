# Runbook — Kafka broker down

> Operator playbook for "the single-broker Kafka instance died and the data plane is unhealthy". Companion docs: [`../tech-spec.md#11-operations`](../tech-spec.md#11-operations), [`../design-docs/kafka-cluster-and-crdb-cluster.md`](../design-docs/kafka-cluster-and-crdb-cluster.md), [`../services/api-gateway.md#8-runbook`](../services/api-gateway.md#8-runbook), [`../services/execution-worker.md#8-runbook`](../services/execution-worker.md#8-runbook).

Today's deployment runs **one Kafka broker per region** (`oj-kafka` in `region.yml`). When it dies, the outbox publisher backs up in CRDB and the worker stops consuming. The 3-broker KRaft cluster (with ISR ≥ 2) is the planned mitigation; see the kafka-cluster design doc.

---

## 1. When to run this

You're paged or noticed one of:

- `kafka-consumer-groups --describe --group execution-worker-pretest` shows growing `LAG` AND `CURRENT-OFFSET = -` on every partition.
- api-gateway logs show repeated `TimeoutException` / `org.apache.kafka.common.errors.NetworkException` lines, AND `oj.gateway.outbox.unpublished` gauge is climbing past 100.
- `docker ps` on the region's VM does not show `oj-kafka` running (or shows it `Restarting (1)`).
- Contestants report "submission accepted but never gets a verdict" en masse — the gateway's 200 PENDING returned because the row + outbox entry committed, but the publisher can't ship it.

If you see this in **only one region**, this is the right runbook. If both regions are affected, also follow [`./crdb-lease-lost.md`](./crdb-lease-lost.md) once Kafka is back — symptoms can compound.

---

## 2. Detection (single command)

```sh
# From the region's VM (oj-region-a or oj-region-b)
sudo docker exec oj-kafka kafka-broker-api-versions --bootstrap-server localhost:29092 2>&1 | head
```

- **Healthy**: prints a long version table starting with `localhost:29092 (id: 1 rack: null) -> (...)`.
- **Broker down**: `Error connecting to node` / `Connection refused` / `command not found if oj-kafka exited`.

`docker ps --filter name=oj-kafka --format 'table {{.Names}}\t{{.Status}}'` is the secondary confirmation.

---

## 3. Diagnose

### 3.1 Why did the container exit?

```sh
# Last 200 lines of broker logs
sudo docker logs oj-kafka --tail 200 2>&1 | tail -80

# Exit code + restart count
sudo docker inspect oj-kafka --format '{{.State.Status}} exit={{.State.ExitCode}} restarts={{.RestartCount}}'
```

Common signatures:

| Log line | Cause |
|---|---|
| `kafka.common.InconsistentClusterIdException` | The Kafka data dir was wiped but Zookeeper still remembers the old cluster ID. Either restore the data dir or wipe ZK state (`docker exec oj-zookeeper zkCli.sh deleteall /kafka`) — the latter forfeits committed offsets. |
| `OutOfMemoryError: Java heap space` | Broker heap exhausted. Check `KAFKA_HEAP_OPTS` in `region.yml`. The container ships with `-Xmx512m` which is tight under bursty load. |
| `org.apache.zookeeper.KeeperException$ConnectionLossException` | Zookeeper container died or is unreachable. Check `docker logs oj-zookeeper --tail 100`. |
| `No space left on device` | `/var/lib/docker/volumes/oj-kafka-data` filled up. See §5 below. |
| (no logs, exit 137) | OOM-killed by the host kernel. `dmesg` will show `Killed process X (java)`. |

### 3.2 Is the producer side actually accumulating?

```sh
sudo docker exec -i oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute="
SELECT count(*) AS unpublished, max(now() - created_at) AS oldest_age
  FROM outbox_events WHERE published = FALSE;
"
```

If `unpublished` is large and `oldest_age` is more than a few seconds, the gateway's polling publisher (`kafkaTemplate.send(...).get(5, SECONDS)`) has been failing for that long. The rows are safe in CRDB; they will drain when the broker is back.

### 3.3 Is the consumer side stuck?

```sh
# Worker container is on the compute VM, not the control-plane VM
sudo docker exec oj-kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --group execution-worker-pretest 2>&1 | head -20
```

`LAG` is the count of un-consumed messages per partition. If LAG is the same now as five minutes ago, the consumer is not making progress. If `CURRENT-OFFSET = -` everywhere, the consumer group has no committed offsets at all — usually because the worker has been crash-looping (see [`../services/execution-worker.md#81-worker-has-no-committed-offsets`](../services/execution-worker.md#81-worker-has-no-committed-offsets), which is the other path that produces this signature).

---

## 4. Mitigate

### 4.1 Restart the broker

```sh
# Order matters: ZK must be up before Kafka starts
sudo docker compose -f /opt/oj/region.yml up -d oj-zookeeper
# Wait ~10s for ZK to reach leader-elected state
sleep 10
sudo docker compose -f /opt/oj/region.yml up -d oj-kafka

# Wait until the broker reports versions
until sudo docker exec oj-kafka kafka-broker-api-versions \
    --bootstrap-server localhost:29092 >/dev/null 2>&1; do
  echo "[wait] kafka not ready yet"; sleep 3
done
echo "[ok] kafka back"
```

Within ~30 s of the broker returning, the api-gateway's outbox publisher tick (`fixedDelay = 1000 ms` by default) will start draining unpublished rows, and the worker's `@KafkaListener` will resume polling from its committed offsets.

### 4.2 Verify the drain

```sh
# Outbox should drain to 0 within a few minutes (depends on backlog size)
watch -n 5 'sudo docker exec -i oj-cockroachdb cockroach sql --insecure \
  --database=onlinejudge --execute="SELECT count(*) FROM outbox_events WHERE published=FALSE;" 2>&1 | tail -5'

# Worker consumer lag should fall to 0
sudo docker exec oj-kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --group execution-worker-pretest 2>&1 | awk 'NR>1 {print $5}' | grep -v '^-$' | paste -sd+ - | bc
```

If after 10 minutes the outbox count is not falling, the publisher itself is not running — bounce `oj-api-gateway` (`docker compose -f /opt/oj/region.yml restart oj-api-gateway`). The publisher is a `@Scheduled` bean inside the gateway JVM; a stuck Tomcat thread can starve it.

### 4.3 ISR validation (3-broker future)

Once the kafka-cluster rollout (design doc §Phase B) lands, the equivalent verification is:

```sh
sudo docker exec oj-kafka kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic submissions.asia-south1.pretest \
  | grep Isr
# Expect Isr: [1,2,3] for every partition. If only [1,2] or [1], the cluster
# is running degraded; writes still succeed because min.insync.replicas=2.
```

A broker that doesn't rejoin the ISR within 5 minutes is the **same** root cause as today's single-broker outage — look at its logs the same way as §3.1.

---

## 5. Rollback / if mitigation went wrong

### 5.1 Restart loop — broker won't stay up

If `docker compose up -d oj-kafka` repeatedly exits 1, the persistent state is corrupted. Decision tree:

- **Acceptable loss**: nothing in-flight matters (e.g. dev VM). Wipe and restart:
  ```sh
  sudo docker compose -f /opt/oj/region.yml stop oj-kafka oj-zookeeper
  sudo docker volume rm oj_oj-kafka-data oj_oj-zookeeper-data
  sudo docker compose -f /opt/oj/region.yml up -d oj-zookeeper oj-kafka
  # Then re-bootstrap topics
  sudo bash /opt/oj/kafka-bootstrap-topics.sh --region asia-south1
  ```
  The outbox in CRDB still has every unpublished row, so the publisher will re-emit them onto the fresh topics. **You will, however, lose every uncommitted message that was in flight on the broker disk** — for the data plane that means verdicts the worker had published but the leaderboard hadn't yet consumed.

- **Unacceptable loss**: this is the production data plane and there are contestant verdicts on disk. Don't wipe. SSH in, copy `/var/lib/docker/volumes/oj_oj-kafka-data/_data` to a sidecar VM, and escalate — log corruption recovery is a `kafka-storage.sh recover` / KRaft procedure that needs the kafka-cluster design doc §Risks to fully define. Page the on-call.

### 5.2 Disk full

```sh
# Verify
sudo df -h /var/lib/docker

# Identify large segments
sudo du -sh /var/lib/docker/volumes/oj_oj-kafka-data/_data/* | sort -h | tail
```

Today's topic retention is `retention.ms = 604800000` (7 days) for hot topics and `2592000000` (30 days) for DLQ — see `infra/scripts/kafka-bootstrap-topics.sh`. If disk pressure is acute and you want to reclaim space without losing in-flight messages, lower retention on the heaviest topic:

```sh
sudo docker exec oj-kafka kafka-configs \
  --bootstrap-server localhost:29092 --alter \
  --topic submissions.asia-south1.pretest \
  --add-config retention.ms=86400000   # 1 day
```

The broker compacts on the next log-cleaner tick (default 5 min). Once disk is free, restore the retention value and grow the underlying disk in terraform.

### 5.3 If you wiped ZK by accident

ZK holds the cluster-id mapping. Wiping it without also wiping Kafka's `meta.properties` yields `InconsistentClusterIdException` on the next boot. Either wipe Kafka too (acceptable-loss path above) OR restore ZK state from the docker volume backup (if you have one — we don't take them today; that's roadmap §3.10-adjacent).

---

## 6. Related incidents

- [`./crdb-lease-lost.md`](./crdb-lease-lost.md) — Kafka and CRDB share the same VM in the single-region topology. A host crash takes both down. The drain order matters: bring CRDB up FIRST (gateway needs it for the outbox), then Kafka.
- [`./multi-region.md#32-one-regions-crdb-node-down--writes-hang`](./multi-region.md#32-one-regions-crdb-node-down--writes-hang) — the analogue for CRDB. Kafka does not yet have a peer-region mirror, so a region-A broker outage does not failover to region B; submissions in region A stall.
- [`../services/api-gateway.md#81-outbox-publisher-falling-behind`](../services/api-gateway.md#81-outbox-publisher-falling-behind) — the publisher's failure mode in detail.
- [`../services/execution-worker.md#82-submissions-piling-up-on-submissionspretest`](../services/execution-worker.md#82-submissions-piling-up-on-submissionspretest) — the consumer's failure mode.

---

## 7. Escalation

- After 15 minutes of broker-down state with the mitigation in §4 not converging, escalate to whoever owns the kafka-cluster rollout (design doc author). The corrupted-state recovery is not yet on autopilot.
- If contestants are mid-contest and the verdict pipeline is stalled, also page contest-service ownership — they may want to extend the contest window via the operator endpoint (see `../services/contest-service.md`).
- Auto-shutdown jobs (`oj-auto-shutdown-control-plane`, `oj-auto-shutdown-compute`) run at 23:00 IST. If a broker outage extends across that boundary, the auto-shutdown will gracefully stop both VMs and the next morning's restart re-runs the bring-up — which is fine as long as you have not asked the operator to keep the VMs up for a launch event.
