# Lesson 3 — DataStream API: State Management & Evolution

In Flink, **state IS the application**. Watermarks, joins, sessionisation,
deduplication, exactly-once sinks — all of it is structured access to
durable per-key or per-operator state. This lesson is about picking the
right kind of state, controlling its lifetime, and surviving schema changes
between deploys.

## How this lesson works

One big exercise built around four stages, each demonstrating a different
flavor of Flink state. Open `exercises/StatefulAnalytics.java` and implement
the four `static` stage methods (and their helper inner classes) one at a
time. The class compiles and `main()` runs as-is — whichever stage is
unimplemented throws `UnsupportedOperationException` when its subgraph is
built or its first record is processed.

Run after each stage:

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.StatefulAnalytics"
```

Reference solution: `solutions/StatefulAnalytics_Solution.java`.

## Mental model

There are **two kinds** of state in the DataStream API:

### Keyed state (Stages 1–3)
Available only after `keyBy(...)`. Flink stores one value per key per
operator and routes records by key hash so the same key always lands on
the same subtask. This is what 90% of business logic uses.

Sub-types:
- `ValueState<T>` — single value per key.
- `ListState<T>` — append-only list per key. Cheap appends on RocksDB.
- `MapState<K,V>` — per-row map. Fast partial updates and iteration.
- `ReducingState<T>` — single value, automatically reduced on `.add()`.
- `AggregatingState<IN,OUT>` — like reducing but with separate input/output types.

### Operator state (Stage 4)
Per-subtask, not per-key. Implement `CheckpointedFunction` to declare
operator state and serialise it on each checkpoint. Two redistribution
modes:
- **even-split** (`getListState`): on rescale, list entries are redistributed
  evenly across the new subtasks.
- **union** (`getUnionListState`): on rescale, every subtask gets the **full**
  list. Use with care — does not scale to large buffers.

---

## Stage 1 — `clickHistory`

Per-user enrichment that tracks:
- a running click count (`ValueState<Long>`)
- the last 5 categories clicked (`ListState<String>`)

For every event, emit `"userId | clicks=N | recent=[a,b,c]"`.

Implement `ClickHistoryEnricher` (a `KeyedProcessFunction`):
1. Declare descriptors and initialise state handles in `open()`.
2. In `processElement`: bump the counter; `recent.add(category)`; trim with
   `recent.update(all.subList(all.size() - 5, all.size()))` if length > 5.
3. Emit the formatted line.

**Bonus question:** Why is `ListState` a better fit than `ValueState<List<String>>`
for "last N items"?

> *Answer:*
> 1. **Append is O(1).** `ListState.add(x)` writes a single entry. With
>    `ValueState<List>` every update *deserializes* the whole list, mutates,
>    *re-serializes* it. On RocksDB that's a full read-modify-write of a
>    multi-KB blob per event.
> 2. **RocksDB has a native list-merge.** `ListState.add()` becomes a Merge
>    op — no read on the write path. `ValueState` is always Get + Put.
> 3. **Smaller incremental checkpoints** — unchanged tail entries don't have
>    to be re-uploaded.
>
> Downside: `ListState` has no built-in size limit — you trim it yourself.

---

## Stage 2 — `spendSummary`

Per-user category counts (`MapState<String, Long>`) and running spend
(`ReducingState<Double>`, reduced via `Double::sum`).

For every event, emit `"userId | spend=X.XX | byCategory={books=2, toys=1}"`.

Implement `SpendSummaryAggregator`:
1. `MapStateDescriptor` and `ReducingStateDescriptor` in `open()`.
2. `processElement`: `catCounts.put(cat, prev+1)`; `spend.add(price)`; build a
   `LinkedHashMap` snapshot from `catCounts.entries()` for the output.

**Bonus question:** When would you still pick `ValueState<HashMap>` over
`MapState`?

> *Answer:* `MapState` wins on partial updates (one row per put), iteration
> without materialising the whole map, and RocksDB compaction.
> `ValueState<HashMap>` can still be the right call when:
> - the map is **tiny and bounded** — serializing a small map in one shot is
>   cheaper than per-key state-backend round trips
> - you need atomic snapshot semantics across multiple keys ("see all entries
>   at time T or none"). MapState's iterators don't offer that under
>   concurrent updates within a slot.

---

## Stage 3 — `sessionCounter`

Per-user session counter that resets when the underlying state expires (TTL
= 3 seconds, `OnCreateAndWrite` so writes refresh it). Uses a paced source
(real processing-time delays) so you can *see* expiry happen.

Expected output (timings approximate):
```
u1 | gap=…ms     | counter=1     ← first click, fresh state
u1 | gap=1000ms  | counter=2     ← within TTL
u1 | gap=5000ms  | counter=1     ← TTL=3s elapsed → null on read → reset
u1 | gap=1000ms  | counter=2
```

Implement `TtlSessionCounter`:
1. Build the TTL config:
   ```java
   StateTtlConfig ttl = StateTtlConfig
       .newBuilder(Time.seconds(3))
       .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
       .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
       .cleanupIncrementally(10, true)
       .build();
   ```
2. Call `desc.enableTimeToLive(ttl)` *before* `getRuntimeContext().getState(desc)`.
3. In `processElement`: `counter.value()` returns null when expired — reset to 0.

**Bonus question:** Why is the default lazy (read-time) cleanup a production
problem? What other strategies does `StateTtlConfig` expose?

> *Answer:* If a key is written once and never read again, the entry stays
> in state forever — state grows unboundedly even though logically
> everything is "expired." This is one of the most common causes of
> unbounded RocksDB growth in production.
>
> Three knobs:
> 1. `cleanupFullSnapshot()` — drop on full checkpoint/savepoint.
> 2. `cleanupIncrementally(size, runCleanupForEveryRecord)` — heap backend.
>    Each access scans `size` extra entries and drops expired ones.
>    Predictable, amortised.
> 3. `cleanupInRocksdbCompactFilter(queryTimeAfterNumEntries)` — RocksDB
>    backend. Plugs a `CompactionFilter` so expired entries die during
>    background compactions. **Standard production recommendation** for
>    Flink + RocksDB. Tune the threshold for high-cardinality keyspaces.
>
> Rule of thumb: never ship TTL'd state to prod without explicitly choosing
> one of these.

---

## Stage 4 — `batchedFlush`

Non-keyed batching mapper backed by operator `ListState`. Records are
buffered in memory and flushed every `batchSize=3` events. The buffer is
persisted to operator state on each checkpoint so it survives restarts.

Implement `BufferedBatcher implements MapFunction<...>, CheckpointedFunction`:
1. Hold an in-memory `List<ClickEvent> localBuffer`.
2. `map(value)`: add to buffer; if `size >= batchSize` emit a `FLUSH` line and
   clear; otherwise emit `"buffering: <category>"`.
3. `snapshotState(ctx)`: `buffered.update(new ArrayList<>(localBuffer))`.
4. `initializeState(ctx)`:
   ```java
   ListStateDescriptor<ClickEvent> desc = new ListStateDescriptor<>(
       "opBuffer", TypeInformation.of(new TypeHint<ClickEvent>() {}));
   buffered = ctx.getOperatorStateStore().getListState(desc);
   if (ctx.isRestored()) for (ClickEvent e : buffered.get()) localBuffer.add(e);
   ```

**Bonus question:** When is union list state the right call instead of
even-split, and what's the danger of using it for buffered records?

> *Answer:*
> Use **union** when every subtask needs the *complete* list on restore. Two
> classic cases:
> 1. Kafka source operators that need every (partition, offset) before
>    deciding which partitions this new subtask should own after a rescale.
> 2. Small global config tables that change rarely and must be on every subtask.
>
> The danger for buffered records: every subtask receives ALL buffered records
> on restore, so a 7-subtask job with 1000 buffered events each replays
> 7000 events instead of 1000 — records get duplicated. Worse, the JobManager
> gathers the entire global list at every checkpoint, so unbounded buffers
> can OOM it.
>
> Rule: union list state is for **small, global** data; even-split is for
> per-subtask data like our buffer.

---

## Bonus theory: schema evolution & the State Processor API

Sooner or later you will want to change a state class — add a field, rename
one, change a type. Flink's behaviour depends entirely on the chosen
serializer:

| Serializer | Add field? | Remove field? | Rename? | Change type? |
|------------|-----------|---------------|---------|--------------|
| **POJO** (default for getters/setters or public fields) | ✅ (defaults to null/0) | ✅ | ❌ | ❌ |
| **Avro** (specific or generic) | ✅ if Avro-compatible | ✅ if Avro-compatible | ✅ via aliases | ✅ if Avro-promotable |
| **Kryo** (fallback for opaque types) | ❌ — opaque blob | ❌ | ❌ | ❌ |

**Practical rule:** if you ever expect to evolve the schema, make the state
class a clean POJO or use Avro from day one. Once you're on Kryo, the only
escape is a savepoint rewrite.

### When schema evolution is not enough: the State Processor API

The **State Processor API** lets you read/write savepoints offline as if
they were a Flink batch job. Use it to:
- bulk-rewrite operator IDs when refactoring a job graph
- migrate state from one type system to another (e.g. Kryo → POJO)
- precompute initial state from historical data and bootstrap a new job

Sketch (don't run — needs a real savepoint):

```java
ExecutionEnvironment batch = ExecutionEnvironment.getExecutionEnvironment();

SavepointReader savepoint =
        SavepointReader.read(batch, "file:///path/to/savepoint", new HashMapStateBackend());

DataSet<UserCounter> existing = savepoint.readKeyedState(
        "the-keyed-operator-uid",
        new UserCounterReader());

// ... transform with normal DataSet operators ...

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
lesson's `pom.xml` so you can experiment.
