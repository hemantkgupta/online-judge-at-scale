# Runbook — CRDB lease lost / api-gateway 503-ing on writes

> Operator playbook for "the CockroachDB node died or lost its range leases and every JVM service that touches CRDB is throwing connection errors." Companion docs: [`../tech-spec.md#5-data-model`](../tech-spec.md#5-data-model), [`../design-docs/kafka-cluster-and-crdb-cluster.md`](../design-docs/kafka-cluster-and-crdb-cluster.md), [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md), [`./multi-region.md#32-one-regions-crdb-node-down--writes-hang`](./multi-region.md#32-one-regions-crdb-node-down--writes-hang).

Today's deployment runs **one CRDB node per region** (`oj-cockroachdb` in `region.yml`). In the two-region topology this gives 2 replicas of every range, so losing one node halts writes (no Raft majority). The 3-node-per-region rollout (kafka-cluster-and-crdb-cluster design doc §CRDB) is the planned mitigation.

This runbook covers two situations: (a) the node container is dead, and (b) the node is up but has lost its range leases and SQL is hanging.

---

## 1. When to run this

- api-gateway logs show `org.postgresql.util.PSQLException: This connection has been closed.` or `Connection refused: oj-cockroachdb/...:26257`.
- Spring Data repository calls throw `JdbcConnectionException` / `CannotGetJdbcConnectionException`.
- api-gateway returns **HTTP 503** on writes (`POST /api/v1/submissions`, `POST /api/v1/auth/signup`) because `SubmissionService.persist()` cannot commit. Read endpoints may still work if Hikari has cached connections that haven't been used yet.
- problem-service `/actuator/health` reports `DOWN` with `db: { status: DOWN, error: PSQLException }`.
- `cockroach node status` returns `is_available=false` OR the command itself errors with `connection refused`.

If the symptom is api-gateway responding **slowly** rather than 503-ing, but `cockroach node status` shows both nodes available, the issue is more likely Kafka backpressure (gateway can't ship outbox rows) — see [`./kafka-broker-down.md`](./kafka-broker-down.md). If it's a global query against a multi-region cluster that's slow, see [`./multi-region.md#33-locality-async-finalization-stuck`](./multi-region.md#33-locality-async-finalization-stuck).

---

## 2. Detection (single command)

```sh
# From the region's VM
sudo docker exec oj-cockroachdb cockroach node status --insecure \
  --host=oj-cockroachdb:26257 2>&1 | head -20
```

Reading the output:

- **Healthy single region**: one row, `is_available=true`, `is_live=true`.
- **Healthy multi-region**: two rows, both with `is_available=true`. Locality column shows `region=asia-south1,zone=local` and `region=us-central1,zone=local`.
- **One node down**: one row missing OR `is_available=false`. Writes from the surviving node hang on REGIONAL BY ROW tables whose home region is the dead node (see multi-region runbook §3.2).
- **Container dead**: command itself errors with `Error: cannot dial server`.

`docker ps --filter name=oj-cockroachdb --format 'table {{.Names}}\t{{.Status}}'` is the secondary confirmation.

---

## 3. Diagnose

### 3.1 Container alive?

```sh
sudo docker inspect oj-cockroachdb --format \
  '{{.State.Status}} exit={{.State.ExitCode}} restarts={{.RestartCount}} oom={{.State.OOMKilled}}'

sudo docker logs oj-cockroachdb --tail 200 2>&1 | tail -60
```

Common signatures:

| Log line / signal | Cause |
|---|---|
| `panic: runtime error: invalid memory address` | Rare CRDB bug; collect the panic + escalate. Restart usually clears it once. |
| `OOMKilled=true`, exit 137 | Host kernel OOM-killer fired. The two-region VMs are e2-medium (4 GB); CRDB plus the rest of the stack is tight. Check `dmesg | tail`. |
| `using too much memory: 2.0 GiB above limit` | CRDB self-throttled. `--max-sql-memory` and `--cache` flags in `region.yml` cap heap; if they're under-sized for the workload the node refuses queries to protect itself. |
| `failed to dial node n2: connection refused` | This node is up but its peer is down. The local node lost quorum for the ranges it doesn't have leases on. |
| `Error: storage engine: no space left on device` | The persistent volume filled. Heavy submissions + 30-day retention on REGIONAL BY ROW tables can fill 25 GB faster than you'd think. |
| `node lost its liveness lease` (in surviving-region log) | Multi-region case — peer node is unreachable but this one is still serving local-region ranges. |

### 3.2 If the container is alive but SQL still hangs

```sh
# Can SQL even reach it?
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge \
  --execute "SELECT 1;" 2>&1 | head

# Inspect range health
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge \
  --execute "SELECT * FROM crdb_internal.ranges WHERE NOT (replicas @> ARRAY[1]) LIMIT 5;"
```

If `SELECT 1` returns immediately, the node is healthy at the SQL gateway layer but specific tables/ranges have lost their leaseholder. If `SELECT 1` itself hangs, the node has lost its system-range lease — full restart needed.

### 3.3 Cross-region case

```sh
sudo docker exec oj-cockroachdb cockroach node status --insecure --host=oj-cockroachdb:26257 \
  | awk 'NR==1 || $0 ~ /is_available/'
```

Both rows visible but one with `is_available=false`: this is the [`./multi-region.md#32-one-regions-crdb-node-down--writes-hang`](./multi-region.md#32-one-regions-crdb-node-down--writes-hang) case. Continue with §4.3 there.

---

## 4. Mitigate

### 4.1 Container dead — restart it

```sh
sudo docker compose -f /opt/oj/region.yml up -d oj-cockroachdb

# Wait until SQL works
until sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge \
    --execute "SELECT 1;" >/dev/null 2>&1; do
  echo "[wait] crdb not ready yet"; sleep 3
done
echo "[ok] crdb back"
```

On a single-region cluster the node rejoins itself and resumes serving in ~10 s. On a multi-region cluster the surviving peer accepts it back into the cluster automatically; allow ~2 minutes for range leases to redistribute.

After CRDB is back, bounce the JVM services that hold idle Hikari connections — they will not auto-reconnect on every operation, so the first request after a long outage can still 503:

```sh
sudo docker compose -f /opt/oj/region.yml restart \
  oj-api-gateway oj-problem-service oj-contest-service oj-leaderboard-service
```

### 4.2 Container alive but ranges have no leaseholder

Force re-acquisition by transferring a synthetic lease and back:

```sh
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute "
SELECT crdb_internal.transfer_range_lease(range_id, 1)
  FROM crdb_internal.ranges
 WHERE database_name = 'onlinejudge' AND lease_holder = 0
 LIMIT 50;
"
```

`lease_holder = 0` means "no leaseholder". The function call assigns the local node as the holder. If this returns instantly with 0 rows, lease loss isn't your problem after all — recheck §3.

A simpler hammer that almost always works: restart the container.

```sh
sudo docker compose -f /opt/oj/region.yml restart oj-cockroachdb
```

This forces re-election. On a single-node single-region cluster this is safe and fast (~10 s downtime). On a multi-region cluster the surviving peer keeps serving its own ranges throughout.

### 4.3 Disk full

```sh
# Verify
sudo df -h /var/lib/docker

# Identify the heaviest tables
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute "
SELECT name, range_count, approximate_disk_bytes
  FROM crdb_internal.tables
 WHERE database_name='onlinejudge' ORDER BY approximate_disk_bytes DESC LIMIT 10;
"
```

The likely offenders are `outbox_events` (no TTL today — roadmap §3.10-adjacent) and `idempotency_keys`. Manual prune of cleanly-published outbox rows:

```sh
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute "
DELETE FROM outbox_events
 WHERE published = TRUE AND created_at < now() - INTERVAL '7 days';
"
```

For completed idempotency rows older than the reconcile window:

```sh
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute "
DELETE FROM idempotency_keys
 WHERE status IN ('completed','poison') AND created_at < now() - INTERVAL '7 days';
"
```

Do not delete `processing` rows — those are owned by an in-flight worker; deleting them lets duplicate verdicts through.

### 4.4 OOM-killed

`region.yml` ships CRDB with `--cache=384MiB --max-sql-memory=384MiB`. On an e2-medium (4 GB) shared with api-gateway (1 GB), problem-service (512 MB), contest/leaderboard (~768 MB each), Kafka (~512 MB), and Zookeeper, the headroom is ~100 MB. Any spike — e.g. a leaderboard query with a wide IN clause — pushes the host into OOM-kill territory.

Short-term: restart and watch. Long-term: bump the control-plane VM to e2-standard-2 (8 GB) — referenced in [`../services/problem-service.md#85-problem-service-heap-pressure--oom`](../services/problem-service.md#85-problem-service-heap-pressure--oom).

---

## 5. Rollback / if mitigation went wrong

### 5.1 Node won't start after restart

If `docker compose up -d oj-cockroachdb` keeps exiting and logs show `using too much memory`, you over-tightened a cache flag earlier. Revert `region.yml`'s `command:` block to:

```yaml
command:
  - start-single-node          # OR start, --join=..., for multi-region
  - --insecure
  - --advertise-addr=oj-cockroachdb:26257
  - --listen-addr=0.0.0.0:26257
  - --cache=384MiB
  - --max-sql-memory=384MiB
  - --locality=region=asia-south1,zone=local
```

(See `infra/gcp/compose/region.yml` for the canonical block per region.)

### 5.2 Data corruption / pebble failure

If logs show `pebble: corruption: invalid block` or `RocksDB: SST file corruption`, the persistent disk is damaged. Decision:

- **Multi-region**: drop this node from the cluster, wipe its disk, let it re-replicate from the surviving region:
  ```sh
  # On the surviving region's VM
  sudo docker exec oj-cockroachdb cockroach node decommission <dead-node-id> \
    --insecure --host=oj-cockroachdb:26257
  # Then on the dead region: stop CRDB, delete the volume, restart — startup script
  # will `cockroach start --join=<peer>` and pull a fresh copy.
  ```
- **Single-region**: irrecoverable without a backup. The cluster is the database; no backup means no recovery. This is one of the reasons the kafka-cluster design doc lists single-node CRDB as a top-severity debt item.

### 5.3 You accidentally deleted live data

`DELETE` against `idempotency_keys` where `status='processing'` will let duplicate verdicts through on the next Kafka redelivery. Mitigation: bounce the worker so it re-reads its offsets; the worker's `claimSubmission` will recreate any rows that are still in-flight. Don't try to "undo" the delete — CRDB has no MVCC `AS OF SYSTEM TIME` rewind here once GC catches up.

---

## 6. Related incidents

- [`./multi-region.md`](./multi-region.md) — every multi-region CRDB incident lives there; this runbook is the "single node, single region" version of the same shape.
- [`./kafka-broker-down.md`](./kafka-broker-down.md) — co-located on the same VM. Host restarts take both down. CRDB must come up before Kafka so the gateway's bean-wiring doesn't crash on a missing `DataSource`.
- [`../services/api-gateway.md#84-submission-status-query-returns-pending-forever`](../services/api-gateway.md#84-submission-status-query-returns-pending-forever) — the second-order failure when CRDB is fine but the cached verdict in Redis is stale; not a CRDB outage, despite looking like one.
- [`../services/api-gateway.md#85-flyway-baseline-error`](../services/api-gateway.md#85-flyway-baseline-error) — schema migration variant of "gateway can't talk to CRDB".

---

## 7. Escalation

- A CRDB node that won't come back after 5 minutes — escalate immediately. Single-node single-region means this is a hard outage.
- If Flyway has marked a migration `success=false` in `flyway_schema_history`, follow [`./multi-region.md#31-v9-flyway-migration-fails-with-no-regions-configured`](./multi-region.md#31-v9-flyway-migration-fails-with-no-regions-configured) — that's the canonical "mark failed, clear, retry" recipe and it generalises.
- Data-corruption suspicion (`pebble: corruption`) without a multi-region peer to recover from is a launch-blocking event. Escalate to whoever owns the data-plane (the kafka-cluster-and-crdb-cluster design doc author).
- If contestants are mid-contest and writes are stalled past 5 minutes, also alert contest-service ownership to extend the contest window via the operator endpoint.
