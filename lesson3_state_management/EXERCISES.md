# Lesson 3 — DataStream API: State Management & Evolution

In Flink, **state IS the application**. Watermarks, joins, sessionisation,
deduplication, exactly-once sinks — all of it is just structured access to
durable per-key or per-operator state. This lesson is about picking the
right kind of state for the job, controlling its lifetime, and surviving
schema changes between deploys.

## What you'll build

| # | Title | Concepts |
|---|-------|----------|
| 1 | ValueState + ListState | per-key counter and bounded history buffer |
| 2 | MapState + ReducingState | per-row map updates and auto-aggregating state |
| 3 | State TTL | `StateTtlConfig`, `UpdateType`, cleanup strategies |
| 4 | Operator State | `CheckpointedFunction`, even-split vs union list state |

## How to run

```bash
cd lesson3_state_management
mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise1_ValueAndListState"
```

Replace the class with whichever exercise (or its `_Solution` under
`com.training.flink.solutions...`) you want to run.

## Mental model

There are **two kinds** of state in the DataStream API:

### Keyed state (Exercises 1–3)
Available only after `keyBy(...)`. Flink stores one value per key per
operator and routes records by key hash so the same key always lands on
the same subtask. This is what 90% of business logic uses.

Sub-types:
- `ValueState<T>` — single value per key.
- `ListState<T>` — append-only list per key. Cheap appends on RocksDB.
- `MapState<K,V>` — per-row map. Fast partial updates and iteration.
- `ReducingState<T>` — single value, automatically reduced on `.add()`.
- `AggregatingState<IN,OUT>` — like reducing but with separate input/output types.

### Operator state (Exercise 4)
Per-subtask, not per-key. Implement `CheckpointedFunction` to declare
operator state and serialise it on each checkpoint. Two redistribution
modes:

- **even-split** (`getListState`): on rescale, list entries are redistributed
  evenly across the new subtasks.
- **union** (`getUnionListState`): on rescale, every subtask gets the **full**
  list. Use with care — does not scale to large buffers.

## Bonus theory: Schema evolution & the State Processor API

Sooner or later you will want to change a state class — add a field, rename
one, change a type. Flink's behaviour depends entirely on the chosen
serializer:

| Serializer | Add field? | Remove field? | Rename? | Change type? |
|------------|-----------|---------------|---------|--------------|
| **POJO** (default for getters/setters or public fields) | ✅ (defaults to null/0) | ✅ | ❌ | ❌ |
| **Avro** (specific or generic) | ✅ if Avro-compatible | ✅ if Avro-compatible | ✅ via aliases | ✅ if Avro-promotable |
| **Kryo** (fallback for opaque types) | ❌ — opaque blob | ❌ | ❌ | ❌ |

**Practical rule:** if you ever expect to evolve the schema, make the
state class a clean POJO or use Avro from day one. Once you're stuck on
Kryo, the only escape is a savepoint rewrite.

### When schema evolution is not enough: State Processor API

The **State Processor API** lets you read/write savepoints offline as if
they were a Flink batch job. Use it to:

- bulk-rewrite operator IDs when refactoring a job graph
- migrate state from one type system to another (e.g. Kryo → POJO)
- precompute initial state from historical data and bootstrap a new job

Sketch (do not run — needs a real savepoint):

```java
ExecutionEnvironment batch = ExecutionEnvironment.getExecutionEnvironment();

// Read an existing savepoint
SavepointReader savepoint =
        SavepointReader.read(batch, "file:///path/to/savepoint", new HashMapStateBackend());

DataSet<UserCounter> existing = savepoint.readKeyedState(
        "the-keyed-operator-uid",
        new UserCounterReader());

// ... transform with normal DataSet operators ...

// Write a NEW savepoint from the transformed data
SavepointWriter
        .newSavepoint(batch, new HashMapStateBackend(), 128)
        .withOperator(
                OperatorIdentifier.forUid("the-keyed-operator-uid"),
                OperatorTransformation
                        .bootstrapWith(existing)
                        .keyBy(uc -> uc.userId)
                        .transform(new UserCounterBootstrapper()))
        .write("file:///path/to/new-savepoint");

batch.execute();
```

The `flink-state-processor-api` dependency is already wired into this
lesson's `pom.xml` so you can experiment if you want.

## What to look for

- Exercise 1: each user's `clicks=N` should monotonically increase, while
  `recent=[…]` stays bounded at 5 entries.
- Exercise 2: `byCategory` map grows as new categories appear; `spend` keeps
  growing; the same number is computed by `ReducingState` without any
  manual aggregation.
- Exercise 3: counter resets to 1 on the click that comes 5 seconds after
  the previous one, because TTL=3s elapsed in the gap.
- Exercise 4: every 3 events you'll see a `FLUSH 3 events: …` line —
  that's the buffer being emitted. Operator state ensures it would survive
  a restart.

Solutions in `solutions/` include answered bonus questions — read them
once you've taken your own swing at the exercise.
