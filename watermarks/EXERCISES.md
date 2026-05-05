# Lesson 4 — Custom Watermark Strategies

Event time is when something *happened*; processing time is when Flink *saw*
it. Watermarks are how Flink turns the messy, out-of-order, late-arriving
real world into deterministic windowed answers. This lesson is about
**picking the right watermark strategy** and **handling lateness** without
silently dropping data.

## How this lesson works

Open `exercises/WatermarkPipeline.java` and implement the four `static` stage
methods one at a time. The class compiles and `main()` runs as-is —
unimplemented stages throw `UnsupportedOperationException` when their
subgraph is built.

Run after each stage:

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.WatermarkPipeline"
```

Reference solution: `solutions/WatermarkPipeline_Solution.java`.

## Source data

12 hand-picked events across windows `[0,5000)`, `[5000,10000)`, and
`[10000,15000)` (5-second tumbling). The data deliberately includes:

- An out-of-order event at `4_000` arriving after `4_500` — within the 2s
  bound, should still land in window 1.
- A **late** event for window 1 (`u5` at `3_000`) arriving after the
  watermark has crossed `8_000`. Within the 2s allowed lateness it should
  trigger an *update* to window 1.
- A **very late** event (`u6` at `1_000`) past even the allowed lateness —
  should appear in the side output, not in the window result.

---

## Stage 1 — `withBoundedOutOfOrderness`

Assign event-time timestamps from `event.timestamp` and a
`forBoundedOutOfOrderness(2s)` watermark strategy.

```java
clicks.assignTimestampsAndWatermarks(
    WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(
            Duration.ofMillis(OUT_OF_ORDERNESS_MS))
        .withTimestampAssigner((event, ts) -> event.timestamp));
```

**Bonus question:** What's the trade-off when picking the out-of-orderness bound?

> *Answer:* Too small → late events are dropped (or worse, sent silently to
> the side-output you forgot to wire), since the watermark advances past
> their timestamp before they arrive. Too large → windows fire late, end-of-
> day reports lag, alerting is delayed. Pick based on observed lateness in
> production (p99 of arrival-vs-event-time gap), and re-tune as the source
> changes.

---

## Stage 2 — `revenuePerCategoryWindowed`

5-second tumbling event-time window keyed by category. Emit
`(category, windowEnd, totalRevenue)`.

```java
clicks.keyBy(c -> c.category)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
      .aggregate(new SumPriceAccumulator(), new EmitWindowSum());
```

A combined `aggregate(AggregateFunction, ProcessWindowFunction)` is the
production-shape pattern — `AggregateFunction` keeps the per-window
accumulator tiny (one `Double`), and the `ProcessWindowFunction` only sees
the final value when the window fires. Cheaper than buffering all events.

**Bonus question:** Why is this combined `aggregate(...)` preferred over
`apply(WindowFunction)` or `process(ProcessWindowFunction)` alone?

> *Answer:* `apply` and `process` (alone) buffer **every** event in window
> state until firing — memory grows with window size and rate. The combined
> `aggregate(AggregateFunction, ProcessWindowFunction)` keeps only the
> running accumulator in state and gives the window function the rich
> context (key, window, side outputs, timers) for the final emit. Use the
> reduce/aggregate-only flavor when you don't need window context.

---

## Stage 3 — `revenueWithAllowedLateness`

Same window, but with an `allowedLateness(2s)` and a side output for events
that arrive past even *that* extended grace period.

```java
SingleOutputStreamOperator<Tuple3<String, Long, Double>> windowed = clicks
    .keyBy(c -> c.category)
    .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
    .allowedLateness(Time.milliseconds(ALLOWED_LATENESS_MS))
    .sideOutputLateData(LATE_EVENTS_TAG)
    .aggregate(new SumPriceAccumulator(), new EmitWindowSum());

DataStream<ClickEvent> late = windowed.getSideOutput(LATE_EVENTS_TAG);
```

Return both via `WindowedWithLate(results, lateEvents)`.

**Expected behaviour:**
- The first `WINDOW-LATE` emission for `books` in window `[0, 5000)` is the
  base sum (12 + 8 + 15 + 20 = 55).
- When `u5`'s late event (price 3, ts 3_000) arrives, the window fires
  *again* with the updated total (58). This is one of Flink's two main
  late-handling mechanisms (the other is bumping out-of-orderness).
- `u6`'s very-late event (price 99, ts 1_000) arrives past 2s lateness →
  it's emitted on the `LATE-SIDE` stream and NEVER updates the window.

**Bonus question:** When should you prefer `allowedLateness` over a larger
`forBoundedOutOfOrderness` bound?

> *Answer:* `forBoundedOutOfOrderness(N)` makes *all* windows fire N
> milliseconds later. `allowedLateness(N)` lets windows fire on time and
> *also* accept updates for N more ms. Choose `allowedLateness` when:
> - downstream consumers can handle updates (Table API retract streams,
>   upsert sinks, idempotent UPSERTs into Postgres).
> - lateness is rare but you want correctness when it happens.
>
> Choose a larger out-of-orderness bound when downstream is append-only and
> can't tolerate restated results.

---

## Stage 4 — `customWithIdleness`

Build a `WatermarkStrategy` (the supplied `PunctuatedSupplier` works, or use
`forBoundedOutOfOrderness` again) combined with `.withIdleness(2s)`.

```java
WatermarkStrategy<ClickEvent> strategy = WatermarkStrategy
    .<ClickEvent>forGenerator(new PunctuatedSupplier())
    .withTimestampAssigner((event, ts) -> event.timestamp)
    .withIdleness(Duration.ofMillis(2_000));
```

**Why idleness exists:** the output watermark of an operator is the
**minimum** of its input watermarks. If one parallel source partition stops
producing, its watermark stays frozen and the entire downstream pipeline
stalls — windows never fire, timers never trigger, even though the *other*
partitions are flowing.

`withIdleness(N)` marks a partition idle after N ms of silence so the
operator's output watermark is computed from the active partitions only.

**Bonus question:** What's the danger of using a very large
`withIdleness` value, and what's the danger of using a very small one?

> *Answer:* Too large → an actually-broken partition (Kafka offsets stuck,
> upstream service down) silently extends watermarks based on the other
> shards. You compute "complete" results that are missing data from the
> stuck partition, and only notice when the partition catches up and floods
> in late events.
>
> Too small → during a normal traffic dip you mark a healthy partition idle
> and may close windows that the partition would have contributed to. Late
> events get dropped (or shipped to the side output) for purely transient
> reasons.

---

## When you're done

Run the full pipeline. Five labelled groups should appear: `WITH-WATERMARKS`,
`WINDOW`, `WINDOW-LATE` (with one *update* for window 1), `LATE-SIDE` (the
very-late event), and `IDLE-AWARE`.

Move on to **Lesson 5 — Table & SQL API** once Stages 2 and 3 feel
comfortable.
