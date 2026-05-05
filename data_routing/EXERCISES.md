# Lesson 2 — DataStream API: Data Routing

This lesson is about **how records move between operators**. The choice of
partitioning strategy affects parallelism, ordering, latency, state
distribution, and skew. Picking the wrong one is one of the most common
production performance bugs.

## Domain

A small `ClickEvent { userId, category, price, timestamp }` (lives in
`model/`). All four stages tag each record with its subtask index using
`SubtaskTagger` (in `util/`) so you can *see* where data goes.

## How this lesson works

Open `exercises/RoutingTour.java` and implement the four `static` stage
methods one at a time. The class compiles and `main()` runs as-is — whichever
stage is unimplemented throws `UnsupportedOperationException` when its
subgraph is built.

Run after each stage:

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.RoutingTour"
```

Reference solution: `solutions/RoutingTour_Solution.java`.

---

## Stage 1 — `demoForwardRebalanceRescale`

Build three subgraphs from the same parallelism-1 source and print under
labels `FORWARD`, `REBALANCE`, `RESCALE`. Source is parallelism-1; downstream
is parallelism-4.

**Expected observations:**
- `FORWARD` — with parallelism mismatch (source=1, map=4), Flink can't do
  true forward and falls back to round-robin. (True forward would keep each
  upstream subtask's records on the matching downstream subtask, enabling
  operator chaining: no serialization, no network, same thread.)
- `REBALANCE` — explicit round-robin across all 4 downstream subtasks.
  Network shuffle, uniform load.
- `RESCALE` — local round-robin within an upstream-to-downstream group. With
  upstream=1 it looks like rebalance; with upstream=2 each upstream subtask
  only round-robins to its 2 child downstream subtasks.

**Bonus question:** When would you intentionally KEEP forward partitioning despite skew?

> *Answer:*
> 1. **Order preservation** — forward keeps records in their original subtask, so
>    per-key (and per-source) ordering is preserved end-to-end. Rebalance/rescale
>    break this — record N+1 may overtake record N if it lands on a subtask with
>    a shorter queue.
> 2. **Operator chaining** — adjacent operators with matching parallelism and
>    forward partitioning can be chained into a single thread, eliminating
>    serialization, network buffers, and thread switches. Inserting a
>    `rebalance()` breaks the chain.
> 3. **Source-level locality** — if the source already partitions by key (e.g.
>    keyed Kafka topics), the data is already balanced. A keyBy/rebalance just
>    adds a network shuffle on top for no gain.

---

## Stage 2 — `demoKeyByHotspot`

Source is 100 events: 90 share `userId = "vip_user"`, 10 are spread across
`u0..u9`. Build two subgraphs:
- `KEYBY` — `keyBy(e -> e.userId)`: one subtask gets ~90 records.
- `REBALANCE` — uniform comparison: roughly 25-25-25-25.

**Bonus question 1:** Why is co-location of records by key a *feature* for
state correctness and a *bug* for load balancing?

> *Answer:* Keyed state is owned by the subtask that handles the key, so
> reads/writes are always local — no cross-subtask coordination, which is what
> makes "running total per user" or "abandoned cart per user" correct under
> parallelism. The flip side: if one key has 90% of the volume, one subtask
> gets 90% of the work. Throughput is bounded by the slowest subtask.

**Bonus question 2:** Three mitigation strategies for hot-key skew?

> *Answer:*
> 1. **Pre-aggregation** — combine at the upstream/source level before the
>    network shuffle (Flink Table/SQL does this as "mini-batch + local-global agg").
> 2. **Salted keys** — replace hot key K with N pseudo-keys K_0..K_{N-1};
>    aggregate per pseudo-key, then re-aggregate the partials downstream.
> 3. **Two-phase keying** — `keyBy(salted_key)` for partial, then
>    `keyBy(real_key)` for final. The standard pattern for skewed group-by.
>
> Other valid answers: side-output the hot key to a dedicated processor via
> custom partitioner; SQL `MINI_BATCH` hints; pick a different key entirely.

---

## Stage 3 — `demoBroadcast`

Replicate a small lookup table to all subtasks via broadcast state. Build:

```
BroadcastStream<CategoryMultiplier> bcast = multipliers.broadcast(MULTIPLIERS);
clicks.connect(bcast).process(new MultiplierApplier()).print("BROADCAST");
```

Implement `MultiplierApplier` (a `BroadcastProcessFunction`):
- `processElement` — read the multiplier for `click.category` from the
  read-only broadcast state; emit `"category=X price=Y * mult=Z = adjusted=W"`.
  If no multiplier yet, emit `"(no multiplier yet) ..."`.
- `processBroadcastElement` — write `(category → multiplier)` into the
  broadcast state (read-write here).

**Bonus question:** What's the cardinality threshold above which broadcast
becomes a bad idea?

> *Answer:* No hard rule, but the formula is
> `total_memory = entry_size * N_entries * parallelism`. Every subtask stores
> a *full* copy. Pragmatic guidelines:
> - up to ~10k entries / a few MB per subtask — fine
> - 10k–100k — workable, profile
> - > 100k or growing — prefer connect+keyBy or async I/O to a key-value store
>
> Also: broadcast state is heap memory (not RocksDB), restores the *full* state
> from checkpoints (slow recovery for large broadcasts), and updates are sent
> to *all* subtasks (so update rate matters too).

**Stream-type combinations:**
- `DataStream + BroadcastStream` → `BroadcastConnectedStream`, processed by
  `BroadcastProcessFunction`.
- `KeyedStream + BroadcastStream` → keyed flavor, processed by
  `KeyedBroadcastProcessFunction`.

This stage uses the non-keyed flavor.

---

## Stage 4 — `demoCustomPartitioner`

Bucket records by price with a custom `Partitioner<Double>`:

| price        | bucket |
|--------------|--------|
| `< 50`       | 0      |
| `< 200`      | 1      |
| `< 1000`     | 2      |
| `>= 1000`    | 3      |

Apply via `partitionCustom(new PriceBucketPartitioner(), e -> e.price)`.
The `KeySelector` extracts the value passed to `Partitioner.partition()`.

**Bonus question:** What's the risk of a custom partitioner that doesn't take
`numPartitions` into account?

> *Answer:* If `partition()` returns a fixed bucket id without modulo'ing by
> `numPartitions`, the same job will fail when redeployed at lower parallelism
> (savepoint/restore at a different parallelism is normal in Flink). At
> parallelism 4, returning bucket id 5 throws `ArrayIndexOutOfBoundsException`.
>
> Fix: `return bucket % numPartitions;`. The `numPartitions` argument is
> provided by Flink at runtime and reflects the operator's *current*
> parallelism.
>
> Subtle: modulo doesn't fix uneven bucket distributions. Custom partitioners
> can express skew but cannot fix it.

---

## When you're done

Run the full job. You should see four labelled groups interleaved: `FORWARD`,
`REBALANCE`, `RESCALE`, `KEYBY`, `BROADCAST`, `BUCKETED`.

Move on to **Lesson 3 — State Management** once Stages 2 and 3 feel
comfortable.
