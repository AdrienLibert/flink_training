# Lesson 18 — Reliability

Streaming jobs run forever. JVMs crash, nodes get evicted, networks
hiccup. Flink's reliability story is built on four primitives that work
together:

1. **Checkpoints** — periodic snapshots of operator state, written to
   durable storage.
2. **Restart strategies** — what to do when a task fails (retry?
   give up? exponential backoff?).
3. **Replayable sources** — sources that, on restart, can be told
   "rewind to cursor N" and re-emit from there.
4. **Idempotent or transactional sinks** — sinks that don't double-write
   when records get re-emitted.

This lesson shows the first three end-to-end with a fault-injecting map.

## How this lesson works

Open `exercises/CheckpointRestart.java` and implement two things:

1. `FlakyMap.map` — throws once on its 12th input.
2. The checkpoint + restart strategy config in `main`.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.CheckpointRestart"
```

Reference: `solutions/CheckpointRestart_Solution.java`.

---

## Stage 1 — `FlakyMap.map`

```java
public Integer map(Integer x) {
    seen++;
    if (seen >= 12 && !haveCrashed) {
        haveCrashed = true;
        throw new RuntimeException("simulated failure at seen=" + seen);
    }
    return x;
}
```

`haveCrashed` is a **static** field — so it survives operator
restart within the same JVM. (In a real cluster restart, the JVM is
new and the static would reset. We're simulating "one-shot"
failure for pedagogy; in production the trigger is something
genuinely external like bad data or OOM.) `seen` is reset to 0 each
restart, but `haveCrashed = true` blocks the second crash.

On crash:
1. Operator throws; runtime cancels the job.
2. Restart strategy waits 1s, then redeploys.
3. **CountingSource state is restored from the latest checkpoint** —
   you'll see `[source] RESTORED cursor=N` for some N ≤ where the
   crash happened.
4. The job resumes from cursor=N. The source replays records
   ≥ N; the map handles them without crashing (haveCrashed=true).

> *Bonus question:* In a real cluster failure (whole JM dies, restart
> on a new node), the static field would reset. How do you get
> idempotent behavior across cluster restarts?
>
> *Answer:* Don't rely on operator-state machinery to skip a "crash
> trigger" — that conflates business logic with fault simulation.
> The right production answer is: check what caused the crash, and
> either route bad data to a DLQ, retry the call idempotently, or
> fail the job and alert. "Crash once and retry" is fine for
> transient failures (network blips), not for poisoned data.

---

## Stage 2 — checkpoint + restart config in `main`

```java
env.enableCheckpointing(200L, CheckpointingMode.EXACTLY_ONCE);
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(1)));
```

The two key parameters:

- `200L` — interval in ms. 200ms here makes the demo snappy; in
  production, 30s–5min based on state size and tolerance.
- `EXACTLY_ONCE` — record-level guarantee. The alternative is
  `AT_LEAST_ONCE`, which uses unaligned barriers and is slightly
  faster but allows duplicates within a checkpoint.

Restart strategies:

- `noRestart()` — fail-fast. Good for batch.
- `fixedDelayRestart(N, delay)` — try N times, wait `delay` between.
- `failureRateRestart(N, interval, delay)` — fail if more than N
  failures in `interval`; otherwise keep retrying.
- `exponentialDelayRestart(...)` — backoff. Recommended default for
  long-running streaming jobs.

> *Bonus question:* Why does the demo set max-attempts to 2 and not
> infinity?
>
> *Answer:* If the bug is deterministic (same input → same crash), an
> infinite retry strategy will loop forever. Setting a finite limit
> means the job actually fails after exhausting retries; alerting
> kicks in; you have a chance to fix the bug. Production jobs
> typically use exponential backoff with a high cap (e.g., 1000
> retries over 10 hours) so transient failures self-heal but
> persistent bugs eventually surface.

---

## Expected output

```
[source]  checkpoint @ cursor=0
[source]  checkpoint @ cursor=4
OUT> 0
OUT> 1
... (up to 11)
[source]  checkpoint @ cursor=10
[source]  checkpoint @ cursor=12
... CRASH ...
[source]  RESTORED cursor=10
[flaky]   RESTORED haveCrashed=true
OUT> 10
OUT> 11
OUT> 12
... continuing past 12 ...
OUT> 29
```

Numbers will vary — the exact cursor at restore depends on which
checkpoint completed before the crash. The key signs of correctness:

- `RESTORED cursor=` shows the source rewound to the last checkpoint.
- `RESTORED haveCrashed=true` (sometimes) shows the flag was
  persisted in time. If false, the job will crash a second time
  (and the second restart should work).

---

## Stage 3 — Theory: exactly-once semantics end-to-end

Three components must align:

1. **Source replayability.** Kafka offsets, file positions, persisted
   cursor — anything where "rewind to checkpoint N" returns the same
   bytes.
2. **State checkpoints.** Already discussed.
3. **Sink commit.** Either:
   - **Idempotent sinks** — re-writing produces the same end state
     (UPSERT with PK, deduplication on a unique key).
   - **Transactional sinks** — Kafka exactly-once producer (KIP-98),
     two-phase JDBC commit, file rename-on-checkpoint-complete.

Drop any link and exactly-once degrades to at-least-once.

> *Bonus question:* What's the typical end-to-end exactly-once
> production pipeline?
>
> *Answer:* Kafka source (replayable via consumer offsets) → Flink
> with checkpoints → Kafka sink with `transactional.id` + EXACTLY_ONCE
> mode. The Kafka sink uses Flink's `TwoPhaseCommitSinkFunction`
> machinery — it pre-commits records to a transaction, only commits
> after Flink confirms the checkpoint completed. A consumer with
> `isolation.level = read_committed` only sees committed records.

---

## Stage 4 — Theory: how big should checkpoints be?

Two metrics to watch in the Flink UI:

- **Checkpoint duration** — the wall time from "start checkpoint" to
  "all subtasks ack'd". If this is > 10% of your interval, you're
  paying significant overhead.
- **Sync time vs async time** — sync time blocks the operator;
  async time runs in the background. Most of total duration should
  be async. If sync time dominates, your operator state is too
  large to checkpoint quickly — consider smaller per-key state, or
  partition the work.

Production rule of thumb:

- Interval = 60s for unaligned + RocksDB.
- Min pause = interval/2 so consecutive checkpoints don't pile up.
- Timeout = 10× interval so a slow checkpoint doesn't fail the job.

```
execution.checkpointing.interval: 60s
execution.checkpointing.min-pause: 30s
execution.checkpointing.timeout: 10min
execution.checkpointing.unaligned: true
state.backend.incremental: true
```

---

## When you're done

Run the job. You should see at least one source restore + at least
one flaky-map restore, then the job completes through cursor=29.

Move on to **Lesson 19 — Observability**.
