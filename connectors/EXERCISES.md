# Lesson 8 — Mastering Connectors

Connectors are the contract between Flink and the outside world. They
own three things: **how data is read** (source), **how data is written**
(sink), and **what happens at checkpoint time**. Get the third part
wrong and you have a beautiful pipeline that silently loses or
duplicates events.

This lesson walks you through writing a tiny **at-least-once** file
sink — the same shape as `BulkWriter`, the JDBC sink, and Kafka
producer at-least-once. We then look at what would change to make it
**exactly-once**.

## How this lesson works

Open `exercises/CustomConnectors.java` and implement `snapshotState`.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.CustomConnectors"
```

Reference: `solutions/CustomConnectors_Solution.java`.

## Source: `CountingSource`

`util/CountingSource.java` is a simple `SourceFunction` that emits
`count` deterministic `Order` rows and persists its cursor in operator
state. Read it before starting — it's the same shape as the legacy
JDBC/Cassandra sources you'd find in older Flink jobs.

---

## Stage 1 — `BatchedFileSink.snapshotState`

The sink buffers up to `batchSize` records in memory and writes a CSV
line per record on flush. The danger is straightforward:

- We accept records into `buffer`.
- We flush only when `buffer.size() >= batchSize`.
- If the JVM dies with 4 records in the buffer, those 4 records were
  emitted by the source (cursor advanced) but never written to the file.

Implement `snapshotState`:

```java
@Override
public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    flush();
    bufferState.update(new ArrayList<>(buffer));
}
```

Two reasons to write it this way:

1. **`flush()` first.** Once a checkpoint completes, the source
   pushes its cursor forward. If we hadn't flushed, those buffered
   records are gone from the source's view *and* not in the file.

2. **Mirror buffer into `bufferState`.** Even after flushing, if the
   JVM crashes between `flush()` and the checkpoint barrier completing,
   on restore we want the buffer to be empty (which it is) — but if
   `flush()` failed mid-way (partial write to the file) we need the
   restored buffer to re-attempt.

**Bonus question:** Why is this only **at-least-once**, not
**exactly-once**?

> *Answer:* If the JVM crashes *after* `flush()` succeeded but *before*
> the checkpoint barrier completes, on restore the source will rewind
> to the previous cursor (`cursor` was checkpointed before `flush`'s
> file writes were known) and re-emit those records — they'll be
> appended a second time to the file. We have no way to distinguish
> "this batch was already committed" from "this batch is new". The
> standard fix is **two-phase commit**: write to a staging file (the
> "pre-commit"), then on `notifyCheckpointComplete` rename to the final
> name (the "commit"). Flink's `TwoPhaseCommitSinkFunction` and the
> Kafka exactly-once producer both work this way.

---

## Stage 2 — Run it

`main` builds `addSource(new CountingSource(20)) → addSink(new
BatchedFileSink(...))` and runs with checkpointing every 500ms. After
the job finishes you should see:

```
[result] wrote NNN bytes to /tmp/orders-out-XXXX.csv
  o0,u0,10.0
  o1,u1,15.0
  ...
  o19,u3,30.0
```

All 20 rows present, in order, no duplicates. Without `snapshotState`
implemented, the last 0–4 rows (depending on timing) will be missing
because they were buffered but never flushed before the source
finished.

**Bonus question:** Why does this work even though we never set a
checkpoint storage location?

> *Answer:* The default checkpoint storage is `JobManagerCheckpointStorage`,
> which keeps state in the JobManager's heap. It's fine for a one-shot
> local run; in production you set
> `state.checkpoints.dir = s3://bucket/path` (or similar) so checkpoints
> survive a JobManager crash.

---

## Stage 3 — Theory: Source V2 / Sink V2

`SourceFunction` and `SinkFunction` are the **legacy** APIs (still
supported in 1.18, deprecated). The modern unified APIs are:

- **Source V2** (`org.apache.flink.api.connector.source.Source`):
  separates `SplitEnumerator` (job-level — coordinates splits) from
  `SourceReader` (subtask-level — reads splits). The same Source code
  runs for both batch and streaming. Used by `KafkaSource`,
  `FileSource`, etc.
- **Sink V2** (`org.apache.flink.api.connector.sink2.Sink`): separates
  `SinkWriter` (per-subtask, accepts records) from optional `Committer`
  (handles two-phase commit). Used by `FileSink`, `KafkaSink`, etc.

Why migrate? Three reasons:

1. **Unified batch/streaming.** The same Source declares whether it is
   bounded; the runtime handles batch as a special case of streaming.
2. **Decoupled split discovery and reading.** Easier to scale
   parallelism and handle dynamic source partitions (Kafka topic
   re-balance).
3. **Built-in two-phase commit.** Sink V2's `Committer` interface is
   the standard place to implement exactly-once — no need to
   re-implement `TwoPhaseCommitSinkFunction` per sink.

For new code: always use V2. For migrating: ship the V2 sink first
(running in parallel with V1 against a new sink target), then drain V1.

**Bonus question:** Why does Source V2 separate `SplitEnumerator` from
`SourceReader`?

> *Answer:* In V1, a `SourceFunction` is per-subtask, and split
> assignment is implicit in how parallelism is set. That works for a
> Kafka source where each subtask owns a fixed partition set, but
> breaks when partitions are added at runtime — there's no central
> coordinator to redistribute. V2's `SplitEnumerator` runs once
> (cluster-wide, on the JobManager) and assigns splits to readers via
> RPC. New partitions show up as new splits, dispatched without
> restarting subtasks.

---

## Stage 4 — Theory: exactly-once at the sink

Three ingredients are required end-to-end:

1. **Replayable source.** Kafka offsets, file positions — anything
   where "rewind to last checkpoint" returns the same bytes.
2. **State checkpoints.** Operator state survives a JVM crash and the
   restored job continues from a known-good point.
3. **Idempotent or transactional sink.** Either each output is uniquely
   identified and the sink ignores duplicates (idempotent: UPSERT into
   Postgres on a primary key), OR the sink groups outputs into a
   transaction that's only visible after the checkpoint completes
   (transactional: Kafka exactly-once, JDBC XA, two-phase file rename).

Drop any one of the three and you fall back to at-least-once at best.

The two-phase commit pattern (in pseudocode):

```
on invoke(record):
    pendingTx.append(record)

on snapshotState(ctx):
    pendingTx.preCommit()           // staging
    operatorState.update(pendingTx)
    pendingTx = newTx()

on notifyCheckpointComplete(id):
    operatorState.preCommittedTx[id].commit()  // visible
    operatorState.preCommittedTx.remove(id)
```

The crucial property: a `preCommit` produces a durable artifact (a
`.tmp` file, an open Kafka transaction) that survives a JVM crash, but
is **not yet visible** to consumers. `commit` is the rename / abort
decision, made only after Flink confirms the checkpoint succeeded.

---

## When you're done

The CSV contains 20 rows in order. Tail it:

```bash
cat /tmp/orders-out-*.csv | head
```

Move on to **Lesson 9 — Workshop: Building Custom Operators**.
