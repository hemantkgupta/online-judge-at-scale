# Runbook — Multi-Region CRDB cluster

> Operator playbook for the 2-VM multi-region GCP deployment. Companion docs: [`../tech-spec.md#10-deployment`](../tech-spec.md#10-deployment), [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md), [`../services/api-gateway.md`](../services/api-gateway.md) (V9 + Flyway baseline coordination).

---

## 1. Fresh cluster bring-up (first time only)

After `tofu apply` provisions `oj-region-a` and `oj-region-b` and both startup scripts have finished installing Docker + Firecracker + materialising `region.yml`, the CRDB cluster needs three one-time steps before api-gateway can boot Flyway V9.

```sh
# 1. SSH into either region VM (region-a is canonical here).
gcloud compute ssh oj-region-a --zone=asia-south1-a --tunnel-through-iap

# 2. cockroach init — joins the cluster, makes node-1 the leaseholder for
#    initial system ranges. Idempotent; safe to re-run, returns
#    "cluster has already been initialized" the second time.
sudo docker exec oj-cockroachdb cockroach init --insecure --host=oj-cockroachdb:26257

# 3. crdb-multiregion-init.sh — declares the cluster's PRIMARY REGION and
#    adds the secondary region. Required BEFORE api-gateway boots Flyway,
#    because V9 SET LOCALITY DDL fails on a single-region database.
sudo bash /opt/oj/crdb-multiregion-init.sh asia-south1 us-central1
```

Expected output of step 3:

```
[crdb-init] ensuring database onlinejudge exists
CREATE DATABASE
[crdb-init] declaring PRIMARY REGION=asia-south1 on database onlinejudge
ALTER DATABASE
[crdb-init] adding secondary region us-central1
ALTER DATABASE
[crdb-init] cluster regions on database onlinejudge:
database     region        primary  secondary  zones
onlinejudge  asia-south1   t        f          {local}
onlinejudge  us-central1   f        f          {local}
[crdb-init] done
```

Once this succeeds, restart api-gateway on both VMs so Flyway picks up the now-multi-region database:

```sh
sudo docker compose -f /opt/oj/region.yml restart api-gateway
```

Tail the api-gateway logs and confirm Flyway emits `Successfully applied 9 migrations to schema "public", now at version v9`. The V9 step takes ~3 minutes because each `SET LOCALITY` triggers an asynchronous range re-layout.

After Flyway lands V9, also run the Kafka topic bootstrap per region:

```sh
# On oj-region-a
sudo bash /opt/oj/kafka-bootstrap-topics.sh --region asia-south1

# On oj-region-b
sudo bash /opt/oj/kafka-bootstrap-topics.sh --region us-central1
```

---

## 2. Verify cluster + schema state

After the bring-up, ground-truth checks:

```sh
sudo docker exec oj-cockroachdb cockroach node status --insecure --host=oj-cockroachdb:26257
# Expect both nodes, locality columns reading
#   region=asia-south1,zone=local  and  region=us-central1,zone=local
# is_available + is_live both `true`.

sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute "
SHOW REGIONS FROM DATABASE onlinejudge;
SELECT name AS table_name, locality
  FROM crdb_internal.tables
 WHERE database_name='onlinejudge' AND schema_name='public' AND state='PUBLIC'
 ORDER BY name;
"
# Expect 11 user tables: GLOBAL on problems/test_cases/users/contests/
# contest_problems, REGIONAL BY ROW on submissions/outbox_events/
# idempotency_keys/refresh_tokens/auth_events.
# flyway_schema_history is REGIONAL BY TABLE IN PRIMARY REGION — that's
# CRDB's default for unrouted tables, it's fine.
```

---

## 3. Common incidents

### 3.1 V9 Flyway migration fails with "no regions configured"

**Symptom.** api-gateway logs show:

```
ERROR: cannot set LOCALITY on a table in a database that has no regions
SQLSTATE: 22023
Migration V9__multi_region.sql failed
```

**Cause.** api-gateway started Flyway before `crdb-multiregion-init.sh` ran. The migration sees a single-region database.

**Fix.**
```sh
# Run the init script
sudo bash /opt/oj/crdb-multiregion-init.sh asia-south1 us-central1

# Flyway marked V9 failed in the history table; clear it
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute "
DELETE FROM public.flyway_schema_history WHERE version = '9' AND success = false;
"

# Restart api-gateway; Flyway retries V9 against the now-multi-region DB
sudo docker compose -f /opt/oj/region.yml restart api-gateway
```

### 3.2 One region's CRDB node down — writes hang

**Symptom.** `cockroach node status` shows one node `is_available=false`. SQL writes from either region take 10+ seconds OR fail with `RangeNotFoundError`.

**Cause.** 2-node cluster has no Raft majority when one node is down. Every range needs ≥ ceil((replicas+1)/2) replicas live to write; with 2 replicas, that's 2. So losing one node halts writes.

**Behaviour observed.**
- GLOBAL tables (problems, users, contests, …) — reads from the surviving region still work via follower-reads on local replicas. Writes block.
- REGIONAL BY ROW tables — reads + writes for ranges whose home region is the SURVIVING node mostly work (depends on which replica was leaseholder). Reads + writes for ranges pinned to the DEAD region block.

**Fix.** Bring the dead node back. Don't try to "fail over" — there's no quorum to fail over to. The 2-node degraded HA is intentional in the design (3-node cluster is the next step; adding a third VM elevates to proper Raft majority).

```sh
gcloud compute instances start oj-region-b --zone=us-central1-a
# After ~2 min Cockroach rejoins; node_status shows both available
# All ranges resume.
```

### 3.3 LOCALITY async finalization stuck

**Symptom.** V9 migration completed but `SHOW JOBS` shows `LOCALITY CHANGE` jobs running for over 30 minutes; some tables still show locality `NONE` even though V9 successfully applied.

**Cause.** A `SET LOCALITY` DDL kicks off an async range re-layout. Under load OR with insufficient cluster capacity, this can take hours. Normal for an empty cluster — ~3 minutes total in our test. If it's truly stuck, there's a range that can't find a placement target.

**Diagnose.**
```sh
sudo docker exec oj-cockroachdb cockroach sql --insecure --execute "
SHOW JOBS WHERE job_type='SCHEMA CHANGE' AND status='running';
SHOW RANGES FROM DATABASE onlinejudge;
"
```

If any range shows `replicas` not matching the table's locality (e.g. a GLOBAL table with only 1 replica), the placement scheduler is stalling. Usually because the cluster doesn't have the locality diversity it expects (e.g. you only have 1 region declared but tables expect ≥ 2). Verify `SHOW REGIONS FROM DATABASE` returns the expected 2 regions.

### 3.4 Need to add a third region

**Goal.** Scale to 3 nodes for proper Raft majority + add a new geographic region.

**Steps.**
1. Provision a third VM via terraform (add a `local.regions.c` block in main.tf following the existing pattern, plus a third `google_compute_address` + `google_compute_instance`).
2. Apply. The new VM's startup script joins CRDB via `--join`.
3. SSH into any VM and run:
   ```sh
   sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute "
   ALTER DATABASE onlinejudge ADD REGION 'europe-west1';
   ALTER DATABASE onlinejudge SURVIVE REGION FAILURE;
   "
   ```
4. CRDB rebalances ranges. Allow ~10 min on a small dataset, longer at scale.

After this, the cluster survives one full region failure.

### 3.5 Tear down + redeploy

```sh
# Full destroy — wipes data + VMs + IPs.
cd infra/gcp/terraform
GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)" tofu destroy -auto-approve

# Re-apply — get a clean 2-region cluster back
GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)" tofu apply

# Then go back to §1 "Fresh cluster bring-up".
```

---

## 4. Local validation (no GCP)

Before deploying changes to the multi-region schema, validate against a local 2-node CRDB cluster:

```sh
# 1. Spin up two CRDB nodes in docker.
docker compose -f <(cat <<EOF
services:
  crdb-a:
    image: cockroachdb/cockroach:v24.1.10
    container_name: oj-test-crdb-a
    command:
      - start
      - --insecure
      - --advertise-addr=crdb-a:26257
      - --listen-addr=0.0.0.0:26257
      - --http-addr=0.0.0.0:8080
      - --locality=region=asia-south1,zone=local
      - --join=crdb-a:26257,crdb-b:26257
      - --cache=128MiB
      - --max-sql-memory=128MiB
    networks: [oj-net]
  crdb-b:
    image: cockroachdb/cockroach:v24.1.10
    container_name: oj-test-crdb-b
    command:
      - start
      - --insecure
      - --advertise-addr=crdb-b:26257
      - --listen-addr=0.0.0.0:26257
      - --http-addr=0.0.0.0:8080
      - --locality=region=us-central1,zone=local
      - --join=crdb-a:26257,crdb-b:26257
      - --cache=128MiB
      - --max-sql-memory=128MiB
    networks: [oj-net]
networks:
  oj-net: { name: oj-multi-region-test }
EOF
) up -d

# 2. Init the cluster.
docker exec oj-test-crdb-a cockroach init --insecure --host=crdb-a:26257

# 3. Declare regions.
DOCKER=docker CRDB_CONTAINER=oj-test-crdb-a \
  bash infra/scripts/crdb-multiregion-init.sh asia-south1 us-central1

# 4. Apply Flyway V1..V9 via the docker image.
docker run --rm --network=oj-multi-region-test \
  -v "$(pwd)/api-gateway/src/main/resources/db/migration:/flyway/sql:ro" \
  flyway/flyway:9.22 \
  '-url=jdbc:postgresql://crdb-a:26257/onlinejudge?sslmode=disable' \
  '-user=root' '-password=' \
  '-baselineOnMigrate=true' '-baselineVersion=0' \
  migrate

# 5. Verify schema localities (see §2 above).

# 6. Tear down.
docker compose -f <(...) down -v
```

Total run time on Docker Desktop: ~5 minutes (3 of which are V9's async finalisation).

This is exactly the procedure used to validate V9 before Phase 2 was committed; see `docs/tech-spec.md#10` Phase 2 entry.
