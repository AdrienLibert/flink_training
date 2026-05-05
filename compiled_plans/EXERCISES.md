# Lesson 6 — Compiled Plans & Table State Evolution

A SQL string is what *you* write; a **compiled plan** is what the planner
produces — a JSON description of every operator, its config, and the
state it owns. This is the key to evolving SQL pipelines in production
**without losing state**: pin the plan, change the SQL only when you're
ready to migrate.

## Why this exists

The Flink optimizer is constantly improving — same SQL, different plan
across versions. If your job's state schema is tied to the operator IDs
in the current plan, an optimizer change can silently break savepoint
restore. **Compiled plans freeze the operator topology**, so an upgrade
keeps using your old plan (and your old state) until you explicitly
recompile.

## How this lesson works

Open `exercises/CompiledPlanLab.java` and implement three methods.
Each writes/reads a JSON plan file in `/tmp`.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.CompiledPlanLab"
```

Reference: `solutions/CompiledPlanLab_Solution.java`.

We use the built-in `datagen` source (20 rows of fake orders) and the
`print` sink, so there's nothing to set up — the plan loads, runs, and
prints to stdout.

---

## Stage 1 — `compileBaseline`

Compile a SQL pipeline (`INSERT INTO sink SELECT ... GROUP BY ...`) into
a JSON plan and write it to disk.

```java
StreamTableEnvironment tEnv = newTEnv();
tEnv.executeSql(CREATE_SOURCE);
tEnv.executeSql(CREATE_SINK);
CompiledPlan plan = tEnv.compilePlanSql(INSERT_BASELINE);
plan.writeToFile(planFile.toFile(), true);   // overwrite = true
```

The resulting JSON is human-readable. Open it after the run — you'll see
`flinkVersion`, `nodes` (each operator with a stable `id`), and
`edges` connecting them. State is keyed by these node IDs.

**Bonus question:** If you don't pin a compiled plan, what happens on a
Flink minor-version upgrade?

> *Answer:* The planner re-optimizes. If it produces a different
> topology — say, by introducing a local aggregate ahead of the global
> one — the new operator IDs don't match the savepoint, and restore
> fails (or, worse, silently restores partial state). Pinning a compiled
> plan turns the topology from a derived artifact into a versioned one.

---

## Stage 2 — `executeFromFile`

Load the JSON and run it. The TableEnvironment must already have the
source and sink registered — temporary tables are **not** baked into
the plan, only their identifiers are.

```java
tEnv.executeSql(CREATE_SOURCE);
tEnv.executeSql(CREATE_SINK);
tEnv.loadPlan(PlanReference.fromFile(planFile.toFile())).execute().await();
```

You should see ~20 rows of `+I[<category>, <revenue>]` and `-U`/`+U`
updates printed to stdout from the `print` sink.

**Bonus question:** Why temporary tables aren't part of the plan?

> *Answer:* Temporary tables have no catalog identity beyond the current
> session. Persisting them inside the plan would couple plan files to
> ad-hoc DDL, defeating the point of a pinned, portable artifact. In
> production you'd register tables in the catalog (Hive, Glue, custom)
> and reference them by name in the plan — same plan JSON, different
> environments.

---

## Stage 3 — `compileFiltered`

Compile a different SQL (`WHERE amount > 25.0` added) to a second JSON
file. The two plans differ in size and in the operator tree (you'll see
a new `Calc` filter node in the JSON). Open both files and diff them
to see what changed.

**Bonus question:** Why is "add a filter" so dangerous in production?

> *Answer:* The filter changes the row count flowing into the GROUP BY,
> which means the per-key accumulators differ. If you swap from the
> baseline plan to the filtered plan via savepoint restore, the
> aggregator state you restore was computed over the *unfiltered* data —
> so the very first emitted row will be wrong (it'll subtract events
> from state that the new filter would have rejected). Schema/operator
> compatibility is necessary but not sufficient; the *meaning* of the
> state must also still match.

---

## Stage 4 — Theory: state-compatible vs state-breaking changes

Compiled plans give you a stable topology, but state compatibility is
finer-grained. Mark each as **safe** (state survives) or **breaking**
(must reset state):

| Change                                                     | Safe? |
| ---------------------------------------------------------- | ----- |
| Adding a new sink that writes the same query result        | safe  |
| Renaming a column in the sink (sink schema only)           | safe  |
| Adding a `WHERE` filter to an aggregation's input          | breaking |
| Changing `SUM(amount)` to `SUM(amount * 1.0)`              | safe  |
| Changing `SUM(amount)` to `SUM(amount) + COUNT(*)`         | breaking |
| Adding a new column to GROUP BY                            | breaking |
| Reordering columns in GROUP BY                             | breaking |
| Bumping Flink minor version with the **same** pinned plan  | safe  |
| Bumping Flink minor version with **recompiled** plan       | depends |

> *Why filters break aggregation state:* the running accumulator includes
> rows the new filter would reject. There's no rewind.
>
> *Why GROUP BY reorders break:* state is keyed by tuple order. `(a,b)`
> hashes differently from `(b,a)`.
>
> *Why scalar transforms (`amount * 1.0`) are safe:* no state involved —
> projection is stateless and applies post-restore.

**Production rule of thumb:** if the change touches anything that owns
state (GROUP BY keys, window definitions, JOIN keys, OVER partitions),
plan a state migration via the State Processor API. If it only touches
projections and filters that don't precede stateful operators, you're
usually fine.

---

## When you're done

You should see:

1. `[Stage 1] wrote plan: /tmp/plan-baseline-... (NNNN bytes)`
2. ~20 rows of `+I` / `-U` / `+U` updates from `print`.
3. `[Stage 3] baseline plan = ... bytes, filtered plan = ... bytes. Plans differ: true`

Move on to **Lesson 7 — Workshop: SQL Query Evolution**, which
ties this together with savepoint restore.
