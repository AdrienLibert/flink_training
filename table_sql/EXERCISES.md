# Lesson 5 — Mastering Table & SQL API

The DataStream API is fine when you want operator-level control, but for
analytical workloads — filters, joins, aggregations, top-N — SQL is far
shorter, declarative, and uses the same planner that powers `flink-sql`,
the SQL Gateway, and `kubectl` SQL workloads. This lesson is about the
**bridge** between DataStream and Table, and the SQL idioms you'll use
constantly: GROUP BY (changelog), TUMBLE (append), and ROW_NUMBER (top-N).

## How this lesson works

Open `exercises/TableSqlBasics.java` and implement the four `static`
methods. The class compiles and `main()` runs as-is — unimplemented stages
throw `UnsupportedOperationException` when their subgraph is built.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.TableSqlBasics"
```

Reference: `solutions/TableSqlBasics_Solution.java`.

## Source data

`OrderSource` emits 12 orders across 3 categories (`books`, `electronics`,
`clothing`) with timestamps spanning 1s–13.5s. The source emits with
`collectWithTimestamp(...)` and ends with `Watermark.MAX_VALUE` so any
event-time windows fire before the job finishes.

---

## Stage 1 — `ordersTable`

Wrap the raw `DataStream<Order>` in a Table that has a watermarked
event-time column. This is the production pattern: declare the schema
**once** at the boundary, then write SQL against it.

```java
Schema schema = Schema.newBuilder()
        .column("orderId", "STRING")
        .column("userId", "STRING")
        .column("category", "STRING")
        .column("amount", "DOUBLE")
        .column("ts", "BIGINT")
        .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(ts, 3)")
        .watermark("event_time", "event_time - INTERVAL '2' SECOND")
        .build();
return tEnv.fromDataStream(raw, schema);
```

**Bonus question:** Why declare the watermark in the `Schema`, not in a
later `SELECT`?

> *Answer:* The watermark is metadata on the source. It needs to flow
> through the planner so downstream operators (windows, temporal joins)
> know which column is event-time. A `SELECT TO_TIMESTAMP_LTZ(...)` in a
> subsequent query produces a regular timestamp column with no watermark
> attached — TUMBLE/HOP TVFs will reject it with "rowtime attribute is
> not found".

---

## Stage 2 — `totalsByCategory`

```sql
SELECT category, SUM(amount) AS revenue
FROM orders
GROUP BY category
```

This is a **non-windowed** GROUP BY. Each new order updates the running
sum for its category, so the result is a **changelog** of inserts +
updates (Flink emits a `-U` retraction followed by a `+U` insert each
time a row changes).

That's why the demo uses `tEnv.toChangelogStream(...)`.
`tEnv.toDataStream(...)` would throw because the output isn't append-only.

**Bonus question:** What's the cost of a non-windowed GROUP BY in state?

> *Answer:* Unbounded — Flink keeps one accumulator per distinct group
> key forever (or until TTL kicks in via
> `table.exec.state.ttl`). For high-cardinality keys (per-user, per-IP)
> this grows without limit. Prefer windowed aggregations whenever the
> business question has a natural time bound; for unbounded aggregations,
> set TTL explicitly.

---

## Stage 3 — `windowedByCategory`

5-second tumbling event-time window per category, computed via the
`TUMBLE` table-valued function (this is the modern syntax — the old
`TUMBLE(...)` group function is deprecated in 1.18):

```sql
SELECT window_end, category, SUM(amount) AS revenue
FROM TABLE(TUMBLE(
  TABLE orders, DESCRIPTOR(event_time), INTERVAL '5' SECOND))
GROUP BY window_start, window_end, category
```

The TVF assigns each row a `window_start`, `window_end`, and `window_time`
attribute, then you GROUP BY the window columns plus your business key.
Because each window fires once when the watermark passes its end, the
output is **append-only** — `toDataStream(...)` works.

**Bonus question:** What does `INTERVAL '5' SECOND` actually do, and why
not just pass `5000`?

> *Answer:* It's a SQL `INTERVAL` literal — the planner reads it as a
> `Duration` of 5 seconds and uses it as the window size. Passing a
> bare `5000` would be a `BIGINT` and the TVF signature wouldn't match.
> SQL standard distinguishes "5 milliseconds of time" from "the integer
> 5000" — the planner enforces it.

---

## Stage 4 — `topSpenderPerCategory` (bonus)

Per-category top spender via the standard SQL top-N pattern:

```sql
SELECT category, userId, total FROM (
  SELECT category, userId, SUM(amount) AS total,
         ROW_NUMBER() OVER (
             PARTITION BY category ORDER BY SUM(amount) DESC) AS rn
  FROM orders
  GROUP BY category, userId
) WHERE rn = 1
```

Output is a **changelog**: as new orders arrive, the top row may flip,
and Flink emits `-U` / `+U` to keep downstream state correct. Open the
exercise's commented `print("TOP-SPENDER")` line once you've implemented it.

**Bonus question:** Why does Flink optimize this specific pattern?

> *Answer:* `ROW_NUMBER() OVER (PARTITION BY k ORDER BY x DESC)` filtered
> on `rn <= N` is recognized by the planner as a top-N query. It uses a
> dedicated `RankFunction` operator that maintains only N rows per
> partition in state — instead of buffering all rows and re-sorting on
> every update. Without this recognition you'd be holding the entire
> per-key history in state forever.

---

## When you're done

Three labelled streams should appear in the output:

- `CAT-TOTAL` — running revenue per category, retract+insert pairs.
- `WINDOW-CAT` — one row per (5s window × category) on watermark advance.
- `TOP-SPENDER` — top user per category, updates as totals shift.

Move on to **Lesson 6 — Compiled Plans & Table State Evolution**.
