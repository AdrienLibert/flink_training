# Lesson 7 — Workshop: SQL Query Evolution

This is a hands-on workshop that builds on **Lesson 6**. The Flink
runtime can restore a job's state from a savepoint as long as the
**operator IDs and state schemas** match. The compiled plan is the
contract — change the SQL such that the plan's stateful operators stay
identical, and you can roll out the change with zero state loss. Change
them, and you must either reset state or migrate it via the State
Processor API.

This lab compiles five SQL variants of the same baseline query and
prints a summary table. You'll inspect the plan JSON to see exactly
what changed.

## How this lesson works

Open `exercises/QueryEvolutionLab.java`. Implement two methods.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.QueryEvolutionLab"
```

Reference: `solutions/QueryEvolutionLab_Solution.java`.

## The five variants

| Label | Change vs baseline | Expected verdict |
| --- | --- | --- |
| V0_baseline | `SELECT category, SUM(amount) GROUP BY category` | — |
| V1_safe_projection | `SUM(amount) * 1.0` | safe |
| V2_safe_filter_after_agg | `WHERE total > 1.0` outside the agg | safe |
| V3_break_filter_before_agg | `WHERE amount > 5.0` inside the agg | breaking |
| V4_break_extra_groupby_key | `GROUP BY category, userId` | breaking |

For each variant the program writes the plan to `/tmp/plan-V*-*.json`
and prints one row of summary stats.

---

## Stage 1 — `compileAll`

For each `(label, sql)` build a fresh `StreamTableEnvironment`, declare
the tables, and compile. Use a fresh env per variant — the `print`
sink can be re-registered without conflict, but the planner caches some
state inside the env you'd rather not share.

```java
StreamTableEnvironment tEnv = newTEnv();
tEnv.executeSql(CREATE_SOURCE);
tEnv.executeSql(CREATE_SINK_KV);
tEnv.executeSql(CREATE_SINK_KKV);
CompiledPlan plan = tEnv.compilePlanSql(sql);
plan.writeToFile(file.toFile(), false);
```

**Bonus question:** Why register *both* sinks in every env, even when
the SQL only uses one?

> *Answer:* `compilePlanSql` validates that every identifier in the SQL
> exists in the catalog. If we only registered `sink_kv`, the V4 variant
> (which writes to `sink_kkv`) would fail to compile with
> `ValidationException: Object 'sink_kkv' not found`. In production this
> mirrors registering all your tables once at session start, then
> compiling many queries.

---

## Stage 2 — `printPlanSummary`

Read each plan JSON as text and report:

- size in bytes
- a node count (count of `"type" :` occurrences — rough but useful)
- whether the JSON contains `"GroupAggregate"` (the stateful op)

```
variant                          |    bytes | nodes | has GroupAggregate
--------------------------------------------------------------------------------
V0_baseline                      |    XXXX  |     N | true
V1_safe_projection               |    YYYY  |     N | true
V2_safe_filter_after_agg         |    ZZZZ  |     N | true
V3_break_filter_before_agg       |    AAAA  |    N+1| true
V4_break_extra_groupby_key       |    BBBB  |    N  | true
```

Open the plan JSONs side-by-side. You'll notice:

1. **V1** — `SUM(amount) * 1.0` — the `GroupAggregate` operator is
   identical to V0; the `* 1.0` lives in a downstream `Calc` projection.
   State of the agg is unchanged → safe.

2. **V2** — `WHERE total > 1.0` after the agg — same `GroupAggregate`,
   plus a new `Calc` filter downstream. Safe.

3. **V3** — `WHERE amount > 5.0` before the agg — the `GroupAggregate`
   is the same operator type, but its **input row stream is different**.
   Restoring the V0 accumulator would mean continuing to add filtered-
   out rows' contributions that V3 should never have counted. Breaking.

4. **V4** — `GROUP BY category, userId` — the `GroupAggregate`'s
   `grouping` field changes from `[category]` to `[category, userId]`.
   The keying scheme is part of the operator's state schema. Breaking.

**Bonus question:** Why is "V3 breaks state even though the operator
type is the same" not enough to make Flink reject the restore at
runtime?

> *Answer:* Flink checks that the operator state's *types* match. It
> can't know that the *meaning* of the rows fed to the same operator
> changed. From the runtime's view, you handed it a stream of rows and
> told it "fold these into the SUM accumulator". Whether those rows
> represent the same business question is your problem, not the
> runtime's. This is why the Flink docs say "compiled plans guarantee
> runtime compatibility, not semantic correctness".

---

## Stage 3 — Theory: the migration recipe

When a change *is* breaking, here's the production recipe:

1. **Take a savepoint** of the running job (the V0 plan).
2. **Open the savepoint** with the State Processor API:
   ```java
   SavepointReader reader = SavepointReader.read(env, savepointPath, new HashMapStateBackend());
   DataStream<Tuple2<String, Double>> oldAcc = reader.readKeyedState(...)
   ```
3. **Transform** the state to match V_new. For V3 (filter added):
   replay the historical events through the new filter and re-fold
   the accumulator. For V4 (split key): fan out each row to the new
   composite key.
4. **Write a new savepoint** via `SavepointWriter`.
5. **Start V_new from the new savepoint**.

> *Bonus question:* For V4 specifically, why can't you mechanically
> derive the new state from the old?
>
> *Answer:* The V0 accumulator stores `(category) -> total`. V4 needs
> `(category, userId) -> total`. There's no way to recover the
> per-userId breakdown from a single per-category total — the
> information was lost when V0 summed across users. You need to
> *replay events* to populate V4, which means keeping the source
> events (Kafka with sufficient retention is the typical answer).

---

## Stage 4 — When state evolution is forbidden

Some changes are not state-evolvable at all without a controlled
double-write window:

- changing the **output** of a sink (the new schema would need a new
  topic/table)
- swapping a **stateless** operator that participates in a fold (e.g.
  the deserializer)
- changing the **time semantics** (processing-time → event-time)

For these you run V_old and V_new in parallel, sinking to two outputs,
let consumers cut over, then drain V_old.

---

## When you're done

The program prints a 5-row table and writes 5 plan JSONs to `/tmp`.
Diff the JSONs (`diff /tmp/plan-V0_*.json /tmp/plan-V1_*.json`) to
build intuition for what the planner emits.

Move on to **Lesson 8 — Mastering Connectors**.
