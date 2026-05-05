# Lesson 21 — Performance & Tuning

Performance work in Flink falls into a small set of categories,
each with a single high-impact lever:

| Symptom | Lever |
| --- | --- |
| GC pauses, low throughput | Object reuse + serializer choice |
| State access slow | RocksDB tuning (block cache, write buffers) |
| Backpressure between operators | Network buffer count |
| Slow checkpoints | Unaligned checkpoints + incremental snapshots |
| Skew (one key dominates) | Mini-batch / salting (Lesson 12) |
| Watermark slippage | `withIdleness`, watermark alignment |
| OOM | Managed-memory split, parallelism vs slots |

This lesson runs one small Java demo for object reuse, and then walks
through the production tuning levers via `configs/rocksdb-tuned.yaml`.

## How this lesson works

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.ObjectReuseDemo"
```

Then read `configs/rocksdb-tuned.yaml`.

---

## Stage 1 — Object reuse

The demo runs the same pipeline twice — once with default copying,
once with `enableObjectReuse()`. Output must be identical:

```
OFF> (a,10)
OFF> (b,20)
...
ON> (a,10)
ON> (b,20)
...
```

If you saw different output (and the pipeline is correctly written),
either object reuse is broken or your pipeline mutates records.

**Bonus question:** When does object reuse **break** a pipeline?

> *Answer:* When an operator stores a reference to an incoming record
> and the record's fields are later read. Without reuse, the next
> record arrives in a fresh object — your stored reference is safe.
> With reuse, the same object instance is overwritten with the new
> record, and your "stored" reference now sees the new fields.
>
> Concrete example:
> ```java
> private List<Order> buffer;
> public void processElement(Order o, ...) {
>     buffer.add(o);     // BROKEN under object reuse — `o` will be overwritten
> }
> ```
> Fix: `buffer.add(new Order(o.id, o.amount, ...))` — copy first.

> *Production rule:* Enable object reuse globally; audit operators
> that hold references. The throughput gain is typically 10-30% for
> serialization-heavy pipelines.

---

## Stage 2 — Theory: RocksDB tuning

RocksDB is the default state backend for jobs with significant keyed
state. Three knobs that matter:

1. **Memory split.** RocksDB uses memory for write buffers, block
   cache, and metadata. Flink's `state.backend.rocksdb.memory.managed
   = true` lets Flink allocate from the `managed memory` pool (the
   one configured by `taskmanager.memory.managed.size`). Otherwise
   RocksDB grabs from the heap and you fight for memory.

2. **Predefined options.** Flink ships profiles:
   - `DEFAULT` — middling, no opinion.
   - `SPINNING_DISK_OPTIMIZED` — for HDD checkpoint storage.
   - `SPINNING_DISK_OPTIMIZED_HIGH_MEM` — same, with bigger caches.
   - `FLASH_SSD_OPTIMIZED` — assumes fast random I/O. Modern default.

3. **Timer service factory.** With many keyed timers (per-key TTL
   timers, per-event windows), the heap-based timer service can OOM.
   `state.backend.rocksdb.timer-service.factory: ROCKSDB` moves them
   to RocksDB.

**Bonus question:** Why are RocksDB timers slower than heap timers?

> *Answer:* Heap timers are a `PriorityQueue<Timer>` — O(log N) for
> register/cancel, in-process memory access. RocksDB timers serialize
> each timer to disk and use a sorted RocksDB column family for the
> priority order. ~5-10× slower per operation, but a job with 100M
> timers will OOM on heap and be fine on RocksDB. Use heap until you
> have evidence of timer pressure.

---

## Stage 3 — Theory: network buffers

Records flow between operators through **network buffers** — small
fixed-size byte arrays managed by Flink's `NetworkBufferPool`. Each
input channel reserves a few buffers; "floating" buffers are shared.
If a downstream operator is slow, its input buffers fill up, and
upstream's output channel can't write — that's backpressure.

The two knobs:

- `taskmanager.network.memory.buffers-per-channel` (default 2). More
  buffers per channel = larger queue absorbing burst, less
  backpressure during normal operation.
- `taskmanager.network.memory.floating-buffers-per-gate` (default 8).
  Floating buffers are shared across input channels; the per-gate
  count is how many can be allocated to one operator's input gate
  at once.

**Bonus question:** Why not just allocate huge per-channel buffers?

> *Answer:* Memory cost. Flink allocates the buffers up front (off-heap),
> not on demand. A job with 100 parallelism × 10 input channels per
> operator × 100 operators × 8 buffers/channel × 32KB/buffer = 256 MB
> per TM, just for network. Increase only when backpressure metrics
> show buffer starvation. The default 2-per-channel is fine for most
> jobs.

---

## Stage 4 — Theory: watermark alignment

For event-time jobs with multiple sources or partitions running at
different speeds, one slow partition holds back the global watermark
and stalls all windows. The fix:

```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
    .withWatermarkAlignment("kafka-source", Duration.ofSeconds(5));
```

This says: "all subtasks in the alignment group `kafka-source` will
not advance their watermark more than 5 seconds ahead of the slowest
peer". A fast partition that gets too far ahead is paused (its source
backpressures itself) until the slow partition catches up. Result:
windows fire on time even when partitions are uneven.

**Bonus question:** What's the cost of watermark alignment?

> *Answer:* Throughput on the fast partitions, when there's a
> persistently slow partition. The alignment paces every partition to
> the slowest. If one partition is genuinely broken (Kafka offset
> stuck, broker down), alignment will pause everything until it
> recovers — exactly what you want for correctness, exactly what you
> don't want for "I have one stuck partition forever".

---

## Stage 5 — Theory: parallelism vs slots

Two concepts often conflated:

- **Parallelism** is how many parallel instances of an operator run.
- **Slots** are the cluster's capacity (one TM has N slots).

A job with parallelism P needs at least P slots (one per parallel
subtask, modulo slot-sharing). The job runs at exactly P parallel
instances — not more, not less, regardless of cluster size.

The trade-off:

- **Higher parallelism** → smaller per-subtask state (better
  RocksDB hit rate), more parallel CPU, smaller checkpoints per
  subtask. But more network shuffles, more state-backend overhead in
  aggregate.
- **Lower parallelism** → less coordination, fewer subtasks to
  serialize-shuffle-deserialize, simpler ops. But fewer cores used,
  larger per-subtask state, hotter RocksDB.

**Bonus question:** What's the right starting parallelism?

> *Answer:* Start at the number of source partitions (Kafka-partitions
> for Kafka source) — the Source V2 API maps one source split to one
> subtask, so going higher than partition count just leaves
> source subtasks idle. After that, profile and tune. Many production
> jobs stabilize at 2-4× the partition count, with the operators
> downstream of source running at higher parallelism for the
> CPU-heavy work and the source operator pinned to partition count.

---

## When you're done

The demo prints two identical streams. Read the YAML. Move on to
**Lesson 22 — Benchmarking & Profiling**.
