# Lesson 22 — Benchmarking & Profiling

"My job is slow" is the start of an investigation, not a conclusion.
This lesson covers the three tools that turn vague slowness into a
specific bottleneck:

1. **Throughput benchmarking** — records per second under controlled
   load.
2. **Flame graphs** (async profiler) — where CPU time goes.
3. **Backpressure inspection** — which operator is blocking.

## How this lesson works

Run the microbenchmark a few times:

```bash
# Default
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.Microbench"

# With object reuse on (Lesson 21)
mvn -q exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.Microbench" \
    -Dexec.args="--reuse"
```

Observe that throughput rises 1.2-2× with reuse on, depending on JIT
warmup and your hardware.

```
[bench] reuse=false N=5000000 wall=NNNNms throughput=NNN,NNN recs/s
[bench] reuse=true  N=5000000 wall=NNNNms throughput=NNN,NNN recs/s
```

---

## Stage 1 — What microbenchmarks tell you (and don't)

Useful for:

- Comparing two code paths on the same machine.
- Establishing an order-of-magnitude baseline for an operator.
- Spotting GC-dominated workloads (throughput swings 2× run-to-run).

NOT useful for:

- Predicting cluster-scale throughput. Networked TM-to-TM exchanges
  dominate at scale, not per-operator CPU.
- Latency. P99 latency tail is where production pain lives;
  microbenchmarks measure mean.
- Backpressure dynamics. A bench with no downstream sink can't show
  backpressure.

> *Bonus question:* Why isn't this microbench using JMH
> (Java Microbenchmark Harness)?
>
> *Answer:* JMH is the right tool for **library-internal benchmarks**
> (a single function's throughput) — it handles JIT warmup, GC quiet
> periods, dead-code elimination, etc. For pipeline-level benchmarks
> we want to see end-to-end behavior including source iteration, sink
> backpressure, network buffer fill — that doesn't fit JMH's
> per-iteration model. We just measure wall-clock for a fixed N.

---

## Stage 2 — Flame graphs

Run the bench under async-profiler:

```bash
./scripts/profile-with-async-profiler.sh com.training.flink.exercises.Microbench
# wait 30 seconds...
open flame.html
```

What to look for in the flame graph:

- **Wide blocks at the top** — hot stack frames. A 30%-wide block at
  `RuntimeContext.getMetricGroup` means metric setup is expensive
  (it shouldn't be).
- **Wide blocks under `Serializer.serialize`** — your codec is
  bottlenecking. Check for Kryo fallback.
- **Wide `Reference.handlePending` / `GC` blocks** — GC pressure.
  Look for excessive allocation in your hot path.
- **No wide block, throughput still low** — likely backpressure or
  I/O-bound; not a CPU profile problem.

> *Bonus question:* Why async-profiler over hprof or JFR?
>
> *Answer:* Async-profiler uses Linux perf_events to sample stacks
> *without* requiring `+UnlockExperimentalVMOptions` or a safepoint
> bias. It samples regardless of safepoint state, including JIT-
> compiled code, native code, and inlined methods. JFR is fine but
> heavier and biased toward safepoints (it'll under-sample
> tight inlined loops). HPROF is essentially deprecated.

---

## Stage 3 — The Flink Web UI

Three tabs that matter:

1. **Job Graph** — running operator status. Backpressure indicator
   per operator (green/yellow/red). The first red operator (closest
   to the source) is your bottleneck.
2. **Subtask Detail** — per-subtask metrics. Look at
   `numRecordsInPerSecond` and `busyTimeMsPerSecond`. A subtask at
   1000 ms busy/sec is fully utilized; one at 200 is mostly idle.
3. **Checkpoints** — last N checkpoints with duration breakdown.
   `Sync time` should be small relative to `End-to-End`. Look for
   stragglers — a checkpoint where one subtask took 10× longer than
   the others is a state-skew indicator.

> *Bonus question:* If every operator shows green (no backpressure)
> but throughput is still low, what's happening?
>
> *Answer:* The source is the bottleneck. Backpressure indicators
> only show downstream pressure; they don't fire when the source
> *itself* can't keep up (Kafka consumer lagging due to broker
> slowness, JDBC source bound on database round-trip). Check
> `numRecordsOutPerSecond` on the source operator and compare with
> the upstream system's emit rate (Kafka broker bytes-out, etc.).

---

## Stage 4 — Theory: backpressure root-causing

Backpressure flows **backwards** from a slow operator. The closest
red operator to the source is the cause:

```
Source ──▶ Map ──▶ KeyBy/Process ──▶ Sink
 GREEN     GREEN    YELLOW            RED
```

Here, the Sink is too slow → its input buffers fill → KeyBy/Process
backpressures → Map upstream sees congested output → ultimately Source
slows down. Fixing the sink fixes everything; "tune the keyBy" would
be wrong.

Common bottlenecks by location:

- **At the source:** consumer not keeping up; check broker metrics.
- **At a `KeyBy`:** skew (Lesson 12) or expensive process function.
- **At an async I/O operator:** capacity too low, or the upstream
  service is slow.
- **At the sink:** sink throughput cap (JDBC commit rate, Kafka
  acks=all timing). Or downstream service overloaded.

> *Bonus question:* Why is "increase parallelism" usually the wrong
> first move?
>
> *Answer:* Parallelism doesn't help if the bottleneck is per-key
> work (one key dominates), per-partition (Kafka has only N
> partitions), or external (sink throughput cap). It DOES help when
> the bottleneck is per-record CPU on a stateless operator. Diagnose
> first; tune second.

---

## Stage 5 — Theory: load-test before you ship

Before a production deploy:

1. **Replay a representative day** of historical data through the new
   pipeline at 2-5× real-time speed. Goal: verify steady-state
   throughput, watermark progression, checkpoint duration.
2. **Inject a 10× burst** for 60 seconds. Goal: verify the pipeline
   recovers (backpressure resolves; checkpoints don't time out).
3. **Kill a TM mid-run.** Goal: verify the restart strategy and
   savepoint-or-checkpoint restore work.

These three smoke tests catch 90% of "this worked in dev" surprises.

---

## When you're done

You've run the microbench in two configs and read the flame-graph
script. Move on to **Lesson 23 — Production K8s Workshop**.
