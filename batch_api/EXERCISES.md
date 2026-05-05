# Lesson 13 — Batch API

A "batch job" in Flink is the same DataStream code with one extra
line: `env.setRuntimeMode(RuntimeExecutionMode.BATCH)`. The runtime
takes the bounded-source hint and makes very different scheduling
decisions. Same code, dramatically different output, dramatically
different fault model.

## How this lesson works

Open `exercises/BatchVsStream.java` and implement `pipeline(...)` — a
6-element source feeding a `keyBy → sum`. Then run main twice (the
driver does this for you), once in STREAMING mode and once in BATCH.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.BatchVsStream"
```

Reference: `solutions/BatchVsStream_Solution.java`.

---

## Stage 1 — `pipeline`

```java
DataStream<Tuple2<String, Integer>> events = env.fromElements(
    Tuple2.of("a", 1),
    Tuple2.of("b", 1),
    Tuple2.of("a", 1),
    Tuple2.of("b", 1),
    Tuple2.of("a", 1),
    Tuple2.of("c", 1)
).returns(Types.TUPLE(Types.STRING, Types.INT));

events
    .keyBy(t -> t.f0)
    .sum(1)
    .addSink(new PrintSinkFunction<>(label, false));
```

That's it — the same code runs in both modes.

## Expected output

**STREAMING** — running totals, one emit per input event:

```
STREAM> (a,1)
STREAM> (b,1)
STREAM> (a,2)
STREAM> (b,2)
STREAM> (a,3)
STREAM> (c,1)
```

**BATCH** — final totals only, one emit per key:

```
BATCH> (a,3)
BATCH> (b,2)
BATCH> (c,1)
```

The difference is **not** an optimization at the API level — the same
`sum(1)` operator is used. The runtime decides what to emit based on
the mode. In BATCH, the keyed agg consumes the entire (sorted)
partition before emitting, so it knows the final value the first time.

---

## Stage 2 — Theory: what changes in BATCH mode

| Aspect | STREAMING | BATCH |
| --- | --- | --- |
| Source | unbounded, watermark-driven | bounded; runtime knows EOF |
| Shuffle | pipelined; consumers run in parallel with producers | blocking; producer finishes a stage before consumer reads |
| Sort | not used; per-key state holds intermediate results | sort-based; partitions sorted before agg, no per-key state needed |
| State backend | full state; checkpoints persist it | in-memory only, scoped to the stage |
| Failure recovery | restore from latest checkpoint | re-run failed stage from upstream blocking shuffle |
| Watermarks | drive event-time ops | irrelevant; data is bounded |
| Side outputs | work | work, but emitted at stage end |

**Bonus question:** Why doesn't BATCH mode use checkpoints?

> *Answer:* Checkpoints exist to capture in-flight state of an
> unbounded stream so failure doesn't require replaying the source
> from the start. Batch jobs have a *different* fault model: each
> stage's output is materialized to disk (the blocking shuffle), so on
> failure you only re-run the affected stage from the previous
> shuffle's persisted output. Checkpoints would just duplicate that
> persistence at higher cost.

---

## Stage 3 — Theory: which jobs to run in BATCH mode

Use BATCH for:

- **Backfills** — replaying historical Kafka data to fill in a new
  metric. Bounded-aware Kafka source + BATCH mode = much faster than
  streaming-mode replay (no per-record watermark coordination, no
  checkpointing overhead, sort-based agg uses far less memory).
- **Big-table joins** — `JOIN` on a bounded dataset where streaming
  mode would buffer everything in state forever.
- **End-of-day reports** — running on a snapshot, where you don't need
  incremental output.

Don't use BATCH for:

- **Anything subscribing to live data** — watermarks and event-time
  windows are STREAMING-only concepts.
- **CEP** — processed in stream order, BATCH mode has no order
  guarantees beyond key partitions.

**Bonus question:** What happens if you set BATCH mode on a job that
reads from a Kafka source without bounded mode?

> *Answer:* Job-graph build-time error. BATCH mode rejects unbounded
> sources. `KafkaSource` has `setBounded(...)` to give it an explicit
> end offset — you can mix: a bounded Kafka backfill job runs fine
> in BATCH; the same connector with `setUnbounded(OffsetsInitializer.latest())`
> only works in STREAMING.

---

## Stage 4 — Theory: SQL is the natural fit

The Table API/SQL is the recommended interface for batch in modern
Flink. Reasons:

- The planner makes batch-vs-streaming decisions automatically based on
  whether the input tables are bounded.
- SQL semantics are batch-native (`GROUP BY`, `ORDER BY`, `JOIN`).
- The same SQL string runs streaming with `tEnv.executeSql(...)` on a
  STREAMING `StreamTableEnvironment`, and runs batch by passing
  `EnvironmentSettings.inBatchMode()`.

This is one of Flink's biggest selling points: write SQL once, run it
on yesterday's data with batch semantics, and on today's data with
streaming semantics, with the same correctness guarantees.

---

## When you're done

You should see:
- 6 lines of `STREAM>` running updates
- 3 lines of `BATCH>` final values

Move on to **Lesson 14 — Pipeline Design Workshop**.
