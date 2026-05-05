# Lesson 9 — Workshop: Building Custom Operators

`map`, `filter`, `process` — these are user-friendly wrappers over the
real machinery: **operators**. Specifically, every DataStream operation
is backed by an `AbstractStreamOperator` subclass that implements
`OneInputStreamOperator<IN, OUT>` (or two-input, source, sink, etc.).
Most of the time the wrappers are perfect. But when you need to
register processing-time timers in code that doesn't naturally fit a
`KeyedProcessFunction`, or you want a per-task counter that doesn't
care about keys, you drop down to the operator API.

This lesson walks you through writing one: a **throughput meter** that
forwards every event AND prints elements-per-tick on a regular
processing-time interval.

## How this lesson works

Open `exercises/CustomOperatorLab.java` and implement three methods on
`ThroughputMeterOperator`.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.CustomOperatorLab"
```

Reference: `solutions/CustomOperatorLab_Solution.java`.

## How to plug it into a stream

Operators are not added via `.map(...)` — they're inserted via
`.transform(name, outTypeInfo, operator)`:

```java
stream.transform(
    "ThroughputMeter",
    TypeInformation.of(Event.class),
    new ThroughputMeterOperator(200))
```

The `name` shows up in the Flink UI's job graph. `outTypeInfo` tells
the runtime the serializer to use downstream — operators don't
auto-derive it the way `.map(lambda)` does.

---

## Stage 1 — `processElement`

```java
@Override
public void processElement(StreamRecord<Event> element) throws Exception {
    counter++;
    output.collect(element);
}
```

Two things to notice:

1. **`StreamRecord<Event>` is the wire-level container.** It bundles
   the user payload with its event-time timestamp and watermark info.
   Inside a `process` UDF you only see `Event`; here you see the wrapper
   directly. That gives you control: you can `output.collect(new
   StreamRecord<>(modifiedEvent, originalTimestamp))` to preserve the
   original timestamp through a transform.
2. **`output` is a field of `AbstractStreamOperator`.** It's the
   typed `Output<StreamRecord<OUT>>`. There's no `Collector<OUT>` to
   pass around — the operator owns the output channel.

**Bonus question:** Why doesn't this use `KeyedStateBackend`?

> *Answer:* Operator state lives per-subtask, not per-key. Our counter
> is exactly that — one number per parallel instance. If you wanted
> per-key throughput you'd extend `AbstractStreamOperator` and also
> implement `Triggerable` to register keyed timers, or just use a
> `KeyedProcessFunction`. The point of dropping to the operator level
> is to access the lower-level primitives directly when the higher
> abstractions don't fit.

---

## Stage 2 — `open`

```java
@Override
public void open() throws Exception {
    super.open();
    counter = 0;
    long now = getProcessingTimeService().getCurrentProcessingTime();
    getProcessingTimeService().registerTimer(now + reportEveryMs, this);
}
```

`open` runs once per parallel subtask, before the first `processElement`.
It's the right place to allocate state, open connections, and register
recurring timers.

`getProcessingTimeService()` returns the `ProcessingTimeService` that
the runtime thread uses for all wall-clock work. **Don't use
`System.currentTimeMillis()`** — the runtime intercepts time in tests
and during chained execution, and your timer would fire at the wrong
moment.

**Bonus question:** Why do we re-register from `onProcessingTime`
instead of using a recurring schedule?

> *Answer:* `registerTimer` schedules a one-shot. The recurring API is
> `scheduleAtFixedRate`, but it has different fault semantics — the
> callback might fire concurrently with `processElement` if the
> previous tick takes too long. Re-registering from inside the callback
> serializes naturally on the operator thread, no concurrency to worry
> about.

---

## Stage 3 — `onProcessingTime`

```java
@Override
public void onProcessingTime(long timestamp) throws Exception {
    System.out.println("[meter] elements in last " + reportEveryMs + "ms = " + counter);
    counter = 0;
    long next = getProcessingTimeService().getCurrentProcessingTime() + reportEveryMs;
    getProcessingTimeService().registerTimer(next, this);
}
```

`timestamp` is the time the timer was scheduled to fire (i.e., what we
asked for in `registerTimer`). It is **not** the current wall-clock
time — under load, `currentProcessingTime` may have moved past
`timestamp` already, in which case "every 200ms" becomes "as fast as
the operator can keep up".

**Bonus question:** What's the failure mode if you reset `counter`
*before* printing?

> *Answer:* You'd print 0 every tick. The `counter` is shared between
> `processElement` and `onProcessingTime`, both running on the
> operator thread, but the order matters: read THEN reset. (Volatile /
> synchronization isn't needed because the runtime guarantees these
> two methods are not invoked concurrently — same operator thread.)

---

## Stage 4 — Theory: when to drop to operator level

**Stay at the UDF level (map / process / KeyedProcessFunction)** for:

- per-key state and per-key timers
- side outputs
- anything that fits naturally in a `Function` interface

**Drop to `AbstractStreamOperator`** for:

- per-task (not per-key) counters and timers
- intercepting `StreamRecord` directly to preserve/modify timestamps
- watermark interception (override `processWatermark`)
- chaining strategy control (`setChainingStrategy`)
- direct access to `MetricGroup` for ultra-tight metric reporting

**Drop to `AbstractInvokable` / `StreamTask`** … never. That's the
runtime engine. If you think you need it, you don't.

---

## Stage 5 — Bonus theory: chaining

`AbstractStreamOperator` exposes `setChainingStrategy(...)`. The four
strategies, in order from "share a thread with neighbors" to "force a
shuffle":

- `ALWAYS` — operator chains with adjacent operators when possible.
  Default for stateless ops like map/filter. No serialization between
  chained operators (just method calls), so it's the fastest.
- `HEAD` — operator can be the start of a chain, but doesn't accept
  upstream chaining. Default for sources.
- `NEVER` — never chains. The operator runs in its own task thread.
  Use when the operator is so expensive it would starve the chain.
- `HEAD_WITH_SOURCES` — operator can chain its sources but is itself a
  chain head. Specialized; rarely used outside Flink's own internals.

For our `ThroughputMeter`, `ALWAYS` is right — it does almost no work
per element, no reason to break the chain.

---

## When you're done

You should see ~12 events go through, with `[meter]` lines printed on
each 200ms tick:

```
OUT> a=0
OUT> b=1
OUT> c=2
[meter] elements in last 200ms = 4
OUT> a=3
...
[meter] elements in last 200ms = 4
...
```

Move on to **Lesson 10 — Efficient Dataflows** (Architecting
Efficient Pipelines section).
