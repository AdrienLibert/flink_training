# Lesson 17 — Deployment: SQL-centric Workloads

Most large Flink shops have **two** kinds of workloads:

1. **A handful of mission-critical pipelines** in DataStream/Java,
   each with its own application-mode cluster.
2. **Many SQL queries** — analysts, BI tools, ad-hoc dashboards —
   that benefit from a shared session cluster + a REST front door.

This lesson covers (2). The tools are the **SQL Gateway** (REST),
the **SQL Client** (REPL on top of the gateway), and the standard
Hive/JDBC catalogs.

## How this lesson works

There's no Java. Read:

- `sql/01_orders_pipeline.sql` — a production-shaped SQL pipeline.
- `manifests/sql-gateway.yaml` — the FlinkDeployment that hosts it.

---

## Stage 1 — The SQL Gateway

Started by the JobManager at port 8083 (configurable). Two endpoints:

| Endpoint | Purpose |
| --- | --- |
| `POST /sessions` | Open a session (returns sessionHandle) |
| `POST /sessions/<id>/statements` | Submit a SQL statement |
| `GET  /sessions/<id>/operations/<opId>/result/<token>` | Poll results |
| `DELETE /sessions/<id>` | Close session |

Sessions are **isolated**: each has its own configurable
`TableEnvironment`, its own temporary tables, its own catalogs. But
all sessions share the **session cluster's slots** — so 100 sessions
running 1-parallelism queries share the same 12 slots.

**Bonus question:** Why doesn't each SQL Gateway session get its own
JobManager?

> *Answer:* That would be the application-mode shape — heavyweight,
> ~30s startup per session. SQL Gateway prioritizes interactive
> latency: open a session in 100ms, run a query in 1s. The trade-off
> is shared blast radius — a runaway query can starve other sessions.
> For mission-critical pipelines, use application-mode FlinkDeployment
> (Lesson 16); for analytics, SQL Gateway is the right shape.

---

## Stage 2 — The SQL Client

A REPL on top of the gateway:

```bash
./bin/sql-client.sh gateway --endpoint sql-gateway:8083
```

Or embedded (own MiniCluster — for dev only):

```bash
./bin/sql-client.sh embedded
```

REPL features that matter in production:

- `SET 'execution.runtime-mode' = 'batch';` — toggle batch on the fly
  for the current session.
- `\i 01_orders_pipeline.sql` — execute a file.
- `EXPLAIN PLAN_ADVICE FOR <query>;` — get the planner's advice
  (e.g., "consider mini-batch", "consider local agg") for a query.
- `SHOW JOBS;` — list jobs running in this session cluster.

**Bonus question:** What's the difference between `;` and `\g`?

> *Answer:* `;` submits the statement and waits for its full result.
> For SELECT queries that emit a changelog, this **never returns**
> (the stream is unbounded). `\g` is identical to `;`. To stop a
> long-running SELECT in the REPL, hit Ctrl+C — the gateway will
> cancel the underlying job. For "fire and forget" use
> `INSERT INTO ... SELECT ...` which submits a job and returns the
> handle without tailing.

---

## Stage 3 — `01_orders_pipeline.sql` walkthrough

Read the file. Note:

1. **`CREATE CATALOG hive`** — Hive Metastore (or AWS Glue / Iceberg /
   custom) gives the gateway a persistent table directory across
   sessions. Without it, `CREATE TABLE` is per-session and disappears
   when the session closes.

2. **`event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'`** — pulls
   the event-time from the Kafka record's metadata timestamp instead
   of from a payload column. This is the standard pattern when the
   producer sets timestamps on the record header.

3. **`PRIMARY KEY (order_id) NOT ENFORCED`** — even on a Kafka source.
   Tells the planner that the topic is keyed (Kafka partition key =
   order_id) so downstream upserts can target it.

4. **`'avro-confluent'`** — uses Confluent Schema Registry for Avro
   schemas. The gateway image must include the
   `flink-sql-avro-confluent-registry` jar in `/opt/flink/lib`.

5. **`${env:PG_USERNAME}`** — environment variable substitution.
   Standard pattern for credentials: store in Kubernetes Secrets,
   project as env vars on the JobManager, reference in SQL.

**Bonus question:** Why is `'connector' = 'jdbc'` enough for upsert
semantics here? What if the planner can't satisfy the PK contract?

> *Answer:* The JDBC connector announces it supports upserts via
> `SupportsUpsert`. The planner verifies the upstream query's
> changelog mode (which is `RETRACT` for the GROUP BY) is compatible
> with the sink's PK declaration. If the upstream produced
> `INSERT_ONLY`, the planner would happily INSERT into the sink. If
> the upstream were `RETRACT` and the sink had no PK, the planner
> would refuse to compile — there's no way to translate retracts into
> a PK-less write without information loss.

---

## Stage 4 — Theory: stateful SQL deployment is harder than DataStream

Three challenges specific to SQL workloads:

1. **Plans drift across Flink versions.** A `1.18 → 1.19` minor bump
   may produce a different operator graph for the same SQL. Lesson 6
   (compiled plans) is how you pin the topology — and SQL workloads
   are where this matters most.

2. **State TTL is global by default.** Set
   `table.exec.state.ttl = 12h` in the gateway's config. Without TTL,
   GROUP BY state grows forever. With TTL, state expires N hours after
   last update — a sane default that you'll want to override per-query
   for queries that need longer retention.

3. **Mini-batch is your friend.**
   `table.exec.mini-batch.enabled: "true"` aggregates updates every
   `mini-batch.size` records or `mini-batch.allow-latency`. The
   planner inserts a local pre-aggregator that combines updates
   before the GROUP BY's network exchange. 5–10× throughput
   improvement on skewed keys, near-zero latency cost (configurable).

**Bonus question:** What's the trade-off with mini-batch?

> *Answer:* Latency. Without mini-batch, every input event triggers
> an immediate update emission. With it, updates batch up to
> `allow-latency` (e.g., 1s) before emitting. For a real-time
> dashboard, 1s lag is invisible; for a fraud detection pipeline,
> it might miss the SLA. Tune to your downstream's tolerance.

---

## Stage 5 — Submitting via FlinkSessionJob with SQL

Operator-managed SQL jobs (since 1.6):

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkSessionJob
metadata:
  name: orders-totals-sql
spec:
  deploymentName: sql-gateway-cluster
  job:
    jarURI: https://artifacts.example.com/flink-sql-runner-1.18.1.jar
    entryClass: org.apache.flink.connector.testframe.scripts.SqlRunner
    args:
      - --sql
      - /opt/flink/sql/01_orders_pipeline.sql
    parallelism: 4
    upgradeMode: savepoint
```

This wraps the SQL file in a "SQL runner" jar that compiles + submits
the SQL on the gateway cluster. The SQL is part of your git repo; the
runner jar is shared infrastructure. Combined with **compiled plans**,
this is the production-grade SQL pipeline shape.

---

## When you're done

Read both files end-to-end. Move on to **Lesson 18 — Reliability**.
