# Lesson 14 — Workshop: Pipeline Design

This workshop ties together everything in the **Architecting Efficient
Pipelines** section: an end-to-end small pipeline that exercises
changelog semantics from source to sink. The setup mimics the shape of
a real production job: data comes in, gets aggregated per key, and
flows out to an upsert sink that downstream systems consume.

## How this lesson works

Open `exercises/EndToEndPipeline.java` and implement three SQL
declarations.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.EndToEndPipeline"
```

Reference: `solutions/EndToEndPipeline_Solution.java`.

---

## Stage 1 — `declareSource`

```sql
CREATE TEMPORARY TABLE events (
    category STRING,
    amount DOUBLE,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '60',
    'rows-per-second' = '20',
    'fields.category.kind' = 'random',
    'fields.category.length' = '3',
    'fields.amount.min' = '1.0',
    'fields.amount.max' = '50.0'
)
```

Three details that matter in production:

1. **`number-of-rows = 60`** — the source is bounded. In production
   this would be a Kafka source with `setBounded(latest())` for a
   backfill, or an unbounded source for a live job.
2. **`rows-per-second = 20`** — paced emission. Lets the rest of the
   pipeline see realistic backpressure dynamics, instead of receiving
   all 60 records in 5ms.
3. **`event_time AS PROCTIME()`** — for this lab we use processing
   time. In production you'd assign event time from a column and a
   `WATERMARK FOR ts AS ts - INTERVAL '2' SECOND` clause.

---

## Stage 2 — `declareSink` (the key idea)

```sql
CREATE TEMPORARY TABLE category_totals (
    category STRING,
    total DOUBLE,
    PRIMARY KEY (category) NOT ENFORCED
) WITH ('connector' = 'print')
```

The `PRIMARY KEY (...) NOT ENFORCED` is the critical line. It tells
the planner: "the rows of this sink are uniquely identified by
`category`; route inserts/updates to the same key". This is the
contract that makes upsert sinks (Kafka upsert, JDBC, Cassandra,
Elasticsearch with `_id`, BigTable) work safely with streaming
aggregation.

Without the PRIMARY KEY, the planner would refuse to write a
GROUP-BY-emitting changelog (which produces `-U/+U` retract pairs)
into a print sink that only accepts inserts. With it, the planner
**collapses** retract pairs into UPSERTs at sink time.

**Bonus question:** What does `NOT ENFORCED` mean?

> *Answer:* "I'm telling you the key is unique; please trust me, don't
> validate it." Flink would otherwise need to enforce uniqueness with a
> dedup operator (extra state). For sources/sinks downstream of a Flink
> aggregation that produces unique-by-key output, the constraint is
> already enforced by construction. `NOT ENFORCED` is the standard
> phrase for "schema-level metadata, no runtime check".

---

## Stage 3 — `runAggregation`

```java
tEnv.executeSql(
    "INSERT INTO category_totals " +
    "SELECT category, SUM(amount) " +
    "FROM events GROUP BY category"
).await();
```

You'll see lines like:

```
+I[abc, 23.5]
+I[def, 41.0]
-U[abc, 23.5]
+U[abc, 51.7]
-U[def, 41.0]
+U[def, 89.0]
...
```

Each `+U` is the new "current total" for that category. `-U` is the
retraction of the previous value. With a real upsert sink (Kafka
upsert connector, JDBC), the connector would emit a single `UPSERT`
per category-update — collapsing the retract+insert pair into one
write.

**Bonus question:** Why does GROUP-BY without windowing produce a
**changelog** stream rather than an append stream?

> *Answer:* The result is unbounded — the SUM for category `abc`
> changes every time a new `abc` event arrives. There is no "this is
> the final answer" moment. Producing append-only would require
> committing to a wrong answer or buffering forever. Flink's solution:
> emit a retract for the old value and an insert for the new, treating
> the result as a row-level changelog. Downstream upsert sinks
> understand this and write a single row update.

---

## Stage 4 — Theory: source → ... → sink contract

The planner reasons over **changelog modes**:

- `INSERT_ONLY` — append stream. Source examples: Kafka with
  no-key. Sink examples: file writers, Kafka without key.
- `UPSERT` — keyed stream where the latest row per key wins. Source:
  Kafka upsert connector. Sink: JDBC, Cassandra,
  Elasticsearch-with-id.
- `RETRACT` — full changelog including `+U/-U` pairs. Source:
  rare (test sources). Sink: Postgres, Kafka with retract codec.

The planner refuses to connect a source whose changelog mode is
"upstream of the sink's required mode". You can't write a retract
stream to an insert-only sink without a query that reduces it (e.g., a
LIMIT, last-value-wins via PK, etc.).

**Bonus question:** Why does Flink let you write an UPSERT into a
RETRACT sink, but not the other way around?

> *Answer:* UPSERT semantics are a strict subset of RETRACT
> semantics (RETRACT can express any UPSERT change as a
> retract-then-insert pair). RETRACT into UPSERT requires a primary
> key on the sink, and the planner has to verify that the source's
> retract+insert pairs always agree on the same key — possible after
> a `GROUP BY pk`, but not in general.

---

## Stage 5 — Theory: where each lesson plugs in

Going back through the section:

- **L10 Efficient Dataflows.** This pipeline auto-chains the
  `events → CalcSource → GroupAggregate → Calc → Sink`. You'd
  `disableChaining()` only if the GroupAggregate were heavy enough to
  starve the source.
- **L11 Data Enrichment.** A real pipeline often has an `AsyncDataStream`
  enrichment between source and aggregation. SQL alternative: a
  `LATERAL TABLE(my_lookup_function(...))` with an async UDF.
- **L12 Data Skew.** If `category` is highly skewed, set
  `table.exec.mini-batch.enabled = true` to enable local pre-aggregation
  before the global GROUP BY.
- **L13 Batch API.** Flip the same SQL to batch with
  `EnvironmentSettings.inBatchMode()` — the planner knows the source
  is bounded (`number-of-rows = 60`) and runs the agg with sort-based
  shuffle, no checkpointing.

---

## When you're done

The `print` sink emits ~60 `+I`/`-U`/`+U` rows over 3 seconds. The
final state per category is the SUM of its events.

This concludes the **Architecting Efficient Pipelines** section.
Next up: **Lesson 15 — Deployment**.
