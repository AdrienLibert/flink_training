# Lesson 10 — Efficient Dataflows

You wrote `map → filter → map → sink`. The runtime didn't run four
separate threads; it folded everything into a single task that calls
your operators back-to-back. That's **operator chaining** — and it's
the difference between "10× faster" and "default" for a streaming
job.

This lesson walks you through inspecting the chain by querying the
StreamGraph and JobGraph yourself.

## How this lesson works

Open `exercises/ChainingLab.java` and implement three pipeline builders.
The driver calls `getStreamGraph()` and `StreamingJobGraphGenerator.createJobGraph()`
and prints two counts:

- `streamNodes` — logical operators in the StreamGraph (one per `map`,
  `filter`, `print`, plus the source).
- `jobGraphTasks` — physical tasks after chain folding.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.ChainingLab"
```

Reference: `solutions/ChainingLab_Solution.java`.

---

## Stage 1 — `buildV0` (default chaining)

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
env.fromElements("a", "b", "c")
    .map(x -> x.toUpperCase())
    .filter(x -> !x.equals("B"))
    .map(x -> "<" + x + ">")
    .print();
return env;
```

Expected output line:

```
V0_default_chained     | streamNodes= 5 | jobGraphTasks= 1
```

Five logical operators (source, two maps, filter, sink). One physical
task — they chained.

**Bonus question:** Why does chaining boost performance?

> *Answer:* Within a chain, output of operator N is fed to operator N+1
> by direct method call — no serialization, no buffer, no thread
> hand-off. The whole chain runs on one thread inside one task. Across
> chains, records are serialized into network buffers (even on the same
> JVM) and deserialized on the receiving side. Chains are typically
> 5–10× faster than equivalent unchained pipelines for stateless
> operators. State accesses are unaffected — they always go through
> the state backend.

---

## Stage 2 — `buildV1` (`startNewChain`)

`startNewChain()` says "start a new chain at this operator" — the
operator joins a fresh chain rather than the current one.

```java
.map(...)
.startNewChain()
.filter(...)
.map(...)
.print();
```

Expected:

```
V1_start_new_chain     | streamNodes= 5 | jobGraphTasks= 2
```

Two physical tasks: `[source → map]` and `[filter → map → print]`.

**Bonus question:** Why use `startNewChain()` in production?

> *Answer:* You separate two operators that would naturally chain
> when one of them is expensive (a heavy parsing op, a remote enrichment
> with `RichMapFunction`) and would otherwise hog the chain's thread,
> blocking the other (cheap) operators. Putting the expensive op alone
> lets you also assign it more parallelism or its own slot-sharing
> group.

---

## Stage 3 — `buildV2` (`disableChaining`)

`disableChaining()` is stronger: the operator chains with **nothing** on
either side. Good for "I want this op in its own task no matter what".

```java
.map(...)
.filter(...).disableChaining()
.map(...)
.print();
```

Expected:

```
V2_disable_one_op      | streamNodes= 5 | jobGraphTasks= 3
```

Three physical tasks: `[source → map]`, `[filter]`, `[map → print]`.

**Bonus question:** When is `disableChaining()` the right call?

> *Answer:* When the operator does I/O that blocks (sync HTTP, JDBC),
> and you want to keep the rest of the pipeline pumping while it's
> waiting. With chaining, a blocking call inside the operator stalls
> the source and downstream both. Without, the operator's task can
> backpressure independently. (For async I/O the better tool is
> `AsyncDataStream.unorderedWait` — Lesson 11.)

---

## Stage 4 — Theory: serializer choice

Flink picks a serializer per type. Pecking order:

1. **POJO serializer** — for classes with public no-arg ctor + getters/
   setters or public fields. Fastest, evolvable (you can add fields).
2. **Avro / Protobuf** — if the type is a generated Avro/PB class.
   Schema-evolvable.
3. **Kryo fallback** — for everything else. Works, but ~2–10× slower
   than POJO; not evolvable (adding a field invalidates state).

The diagnostic is simple: run with `env.getConfig().disableGenericTypes()`
in dev. Flink will throw at job-graph time for any type falling back to
Kryo. Fix the type to be a clean POJO, or register a custom serializer.

```java
env.getConfig().disableGenericTypes();
// throws: java.util.HashMap is not a POJO type and falls back to Kryo
```

**Bonus question:** What's special about a Flink POJO?

> *Answer:* Public no-arg constructor. All fields are either public OR
> have public getter/setter pairs. No final non-public fields. If your
> class has a `private final ...` field, Flink can't reflectively set
> it, falls back to Kryo. (You can check: build a `TypeInformation` for
> the class via `TypeInformation.of(...)` — if the result is
> `GenericTypeInfo`, you fell back; if it's `PojoTypeInfo`, you didn't.)

---

## Stage 5 — Theory: slot sharing

A "slot" in Flink is a TaskManager memory partition that can run one
parallel instance of each task in the job. By default all tasks share
the same slot — so a 5-stage job with parallelism 4 fits in 4 slots,
not 20. You opt out with `.slotSharingGroup("name")`.

When to break the default:

- The pipeline has stateful operators that need a lot of off-heap
  memory (RocksDB) and stateless operators that don't. Different
  groups → different slots → different memory footprints.
- A side-job (Kafka source from a different topic, async enrichment)
  has wildly different parallelism. Forcing them into the same slot
  starves whichever is the busier.

Slot sharing is invisible to the **streamNodes / jobGraphTasks** counts
we measured — it's a deployment-time concept, not a graph-build-time
one.

---

## When you're done

```
V0_default_chained     | streamNodes= 5 | jobGraphTasks= 1
V1_start_new_chain     | streamNodes= 5 | jobGraphTasks= 2
V2_disable_one_op      | streamNodes= 5 | jobGraphTasks= 3
```

Move on to **Lesson 11 — Data Enrichment**.
