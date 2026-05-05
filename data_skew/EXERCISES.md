# Lesson 12 — Data Skew

A keyed pipeline only scales as well as its **most loaded key**. If
90% of your events land on `VIP`, then `keyBy("VIP")` sends 90% of the
work to a single subtask. The other parallel subtasks idle. Adding
parallelism doesn't help — Flink's keyBy uses hash partitioning, not
load-aware routing.

This lesson runs the same pipeline two ways:

- **V0 naive** — `keyBy(key) → reduce`. The subtask owning `hash("VIP")`
  sees ~90% of records.
- **V1 salted** — append a small random salt (`"VIP#3"`), `keyBy` on
  the salted key, then reduce. Now VIP events spread across all
  subtasks.

## How this lesson works

Open `exercises/SkewLab.java` and implement:

1. `SaltedKeyBuilder.map` — append `"#" + random[0..SALT_BUCKETS)`.
2. `runSalted()` — wire it into the pipeline.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.SkewLab"
```

Reference: `solutions/SkewLab_Solution.java`.

The `SubtaskCountReporter` chained after the `reduce` prints how many
records each parallel subtask received. Compare V0 vs V1.

---

## Stage 1 — `SaltedKeyBuilder`

```java
public Tuple2<String, Long> map(Tuple2<String, Long> in) {
    return Tuple2.of(in.f0 + "#" + random.nextInt(SALT_BUCKETS), in.f1);
}
```

That's it. The random salt is drawn per-event, so two `VIP` events
likely land on different salted keys. The hash distribution of
`{VIP#0, VIP#1, ..., VIP#7}` across the 4-way `keyBy` is much more
even than the hash of `{VIP, u1, u2, u3}`.

**Bonus question:** Why is `SALT_BUCKETS = 8` enough for parallelism 4?

> *Answer:* You want enough buckets that the hash-mod-parallelism
> distribution is uniform. With 4 parallel slots, 4 buckets is the
> minimum but only works if the hashes happen to map to all 4 slots
> (often they don't — birthday paradox in reverse). A rule of thumb is
> 2× to 4× parallelism. Going much higher (e.g., 64 buckets) gives
> diminishing returns and grows the state of the global agg in phase
> 2.

---

## Stage 2 — `runSalted`

```java
events(env)
    .map(new SaltedKeyBuilder())
    .keyBy(t -> t.f0)
    .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
    .map(new SubtaskCountReporter("V1_salted"))
    .addSink(new DiscardingSink<>());
```

Expected output:

```
[V0_naive] subtask 0 saw 3590 records
[V0_naive] subtask 1 saw 130 records
[V0_naive] subtask 2 saw 150 records
[V0_naive] subtask 3 saw 130 records
[V1_salted] subtask 0 saw ~1000 records
[V1_salted] subtask 1 saw ~1000 records
[V1_salted] subtask 2 saw ~1000 records
[V1_salted] subtask 3 saw ~1000 records
```

(Numbers will vary slightly per run; the V0 imbalance and V1 balance
are the point.)

**Bonus question:** Why does `reduce()` over a salted key still emit
**every** input event downstream — won't that re-create skew at
phase 2?

> *Answer:* Yes, it would — and that's why salting is paired with
> **windowed aggregation** in production. With
> `keyBy(saltedKey).window(...).reduce(...)`, phase 1 only emits 1
> record per `(saltedKey, window)`, so phase 2 sees ~SALT_BUCKETS
> records per original key per window — small enough to land on a
> single subtask without re-creating the skew. This lesson uses
> non-windowed reduce just to make the per-subtask record counts
> visible; in production the salt + window combination is the actual
> pattern.

---

## Stage 3 — Theory: phase 2 — re-aggregating

After phase 1 produces (saltedKey, partial), phase 2 strips the salt
and re-aggregates per original key:

```java
phase1
    .windowAll(...)  // OR keyBy(originalKey).window(...)
    .reduce((a, b) -> Tuple2.of(stripSalt(a.f0), a.f1 + b.f1))
```

Two important variants:

- **Per-key phase 2** — `keyBy(originalKey).window(...).reduce(...)`.
  The VIP subtask in phase 2 sees one record per salt bucket per
  window (8 records per VIP per window) — small, no skew.
- **Global phase 2** — `windowAll(...)`. Forces parallelism 1 on the
  final stage. Fine when the output is small (one record per window),
  but loses parallelism for the final emit.

**Bonus question:** When can you skip phase 2 entirely?

> *Answer:* When downstream consumers know to interpret the salted key
> and aggregate themselves — i.e., the salt is part of the published
> schema. Some teams choose this for high-skew analytics where the
> downstream is a query engine (BigQuery, ClickHouse) that handles the
> rollup faster than another Flink stage. The salt becomes a "sub-key"
> column.

---

## Stage 4 — Theory: detecting skew in production

You don't always know in advance which key will spike. Three signals:

1. **Per-subtask record counts** — The Flink UI exposes "Records sent"
   and "Records received" per subtask under each operator. If the
   spread is > 5×, you have skew. (Or use the
   `numRecordsInPerSecond` metric.)
2. **Backpressure** — A skewed subtask can't keep up; its input
   buffers fill; upstream operators backpressure. The UI's
   backpressure indicator points right at it.
3. **State size** — Open the savepoint with the State Processor API,
   group by subtask, look for outliers. A skewed key has more state.

**Bonus question:** What's the **first** thing to check when the
"top 1% of keys" are skewed but you can't change the schema?

> *Answer:* Local pre-aggregation **without** a salt — sometimes
> called "miniBatch" in Flink Table API. Set
> `table.exec.mini-batch.enabled = true` and
> `table.exec.mini-batch.size = 1000`. The planner inserts a local
> agg before the keyBy that combines records into one update per
> key per mini-batch, reducing the *volume* sent to the skewed
> subtask. Doesn't move work, but reduces it. Free win for SQL.
> For DataStream, the equivalent is a custom `bundle` operator that
> emits per (window | count | timer).

---

## When you're done

V0 numbers are dramatically uneven; V1 numbers are within ~10% of each
other.

Move on to **Lesson 13 — Batch API**.
