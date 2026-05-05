# Lesson 1 — DataStream API: End-to-end E-commerce Analytics

## Setup

You'll need:
- Java 11+
- Maven
- Apache Flink 1.18.x dependencies (already in `pom.xml`)

## Domain

You're building a single Flink job that processes a stream of `ClickEvent`s and
a slowly-changing reference stream of `UserProfile`s. The job has six output
channels — see the print labels in `main()`.

```java
public class ClickEvent {
    public String userId;
    public String productId;
    public String category;     // "electronics", "books", "clothing"
    public double price;
    public long timestamp;      // event time in millis
    public String action;       // "view", "add_to_cart", "purchase"
}
public class UserProfile { public String userId; public String tier; }
```

## How this lesson works

Unlike a series of small disconnected exercises, this lesson is **one big
exercise** built incrementally. Open `exercises/EndToEndAnalytics.java` and
implement the five `static` stage methods one at a time. The class compiles
and `main()` runs as-is — whichever stage you have not yet implemented will
throw `UnsupportedOperationException` when the job graph is built, telling
you exactly which stage to tackle next.

Run after each stage:

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.EndToEndAnalytics"
```

Reference solution: `solutions/EndToEndAnalytics_Solution.java`.

---

## Stage 1 — `purchaseTuples`

Filter to `"purchase"` events, then map each to a `Tuple2<String, Double>` of
`(category, price)`.

**Skills:** `filter`, `map`, `.returns(...)`

**Why `.returns(...)`?** Java erases generic types from lambdas. Flink can't
infer that `e -> new Tuple2<>(e.category, e.price)` produces
`Tuple2<String, Double>` — without `.returns(Types.TUPLE(...))` you'll get
`InvalidTypesException` at job-graph build time.

---

## Stage 2 — `revenuePerCategory`

Compute the running revenue total per category as a continuous stream.

**Skills:** `keyBy`, `reduce`, stateful aggregation.

**Tip:** build on Stage 1 — call `purchaseTuples(clicks)` then `keyBy(t -> t.f0).reduce(...)`.

**Bonus question:** Why does the output show **multiple** totals for the same
category instead of one final number?

> *Answer:* Streams are conceptually unbounded — there is no "end of input" at
> which to compute a single final answer. Every record updates the keyed state
> and emits the new running total. To get one number per category you need a
> notion of completeness: a window (tumbling/sliding/session) that closes and
> emits the slice's result. In the Table API this same stream is a *retract
> stream*; in the DataStream API the sink sees an append-only sequence of
> snapshots.

---

## Stage 3 — `abandonedCartAlerts`

Emit an `Alert(userId, "abandoned cart")` for every user who added an item to
their cart but did not purchase within `ABANDONMENT_TIMEOUT_MS` (event time).

**Skills:** `WatermarkStrategy`, `KeyedProcessFunction`, `ValueState`,
event-time timers.

**Steps:**
1. Assign timestamps + watermarks (`WatermarkStrategy.forMonotonousTimestamps()`).
2. `keyBy(userId)` and call `.process(new AbandonedCartDetector())`.
3. In `AbandonedCartDetector`:
   - Hold a `ValueState<Long>` for the currently-registered timer.
   - On `add_to_cart`: delete the previous timer if any, register a new one for
     `ctx.timestamp() + ABANDONMENT_TIMEOUT_MS`, and update state.
   - On `purchase`: delete the timer and clear state.
   - In `onTimer`: emit the alert and clear state.

**Bonus question 1:** Why must this use **event-time** timers (not processing-time)?

> *Answer:* With a bounded source like `fromElements(...)`, processing-time
> timers never fire — when the source ends, pending processing-time timers are
> silently discarded. Event-time timers get one final flush via a
> `Long.MAX_VALUE` watermark before shutdown. Beyond bounded sources, event
> time is also what you want in production: it makes the job deterministic and
> replayable.

**Bonus question 2:** Should a second `add_to_cart` reset the timer or let the
original keep ticking?

> *Answer:* Reset (the choice this exercise implements). Real-world abandonment
> is about *inactivity*; a user who keeps adding items is engaged and should
> not be alerted. Equivalent alternative: keep all timers alive and, in
> `onTimer`, only emit if the fired timestamp matches the one currently in
> state. Both are correct; the docs slightly prefer the second because
> deleting timers has cost.

---

## Stage 4 — `routeByValue`

Split the input stream into three channels in a single pass:
- main: purchases with `price > HIGH_VALUE_THRESHOLD`
- side `REGULAR_TAG`: purchases with `price <= HIGH_VALUE_THRESHOLD`
- side `NON_PURCHASE_TAG`: any non-purchase event

Return a `RoutedClicks` holder containing all three.

**Skills:** `ProcessFunction`, `OutputTag`, side outputs.

**Bonus question:** Why does `OutputTag<ClickEvent>` need the
anonymous-subclass form `new OutputTag<ClickEvent>("name") {}`?

> *Answer:* Without the trailing `{}`, the type parameter is erased at runtime
> and Flink throws `InvalidTypesException` when it tries to choose a
> serializer. The anonymous subclass preserves its parameterized supertype in
> the bytecode `Signature` attribute, which Flink reads via reflection. Same
> trick as Guava's `TypeToken` and Jackson's `TypeReference`.

---

## Stage 5 — `enrichWithTier`

Enrich each `ClickEvent` with the user's current tier from the profile stream.
If a click arrives before any profile, emit `tier = "unknown"`.

**Skills:** `connect`, `KeyedCoProcessFunction`, `ValueState`.

**Steps:**
1. `keyBy(userId)` on both streams.
2. `keyedClicks.connect(keyedProfiles).process(new TierEnricher())`.
3. In `TierEnricher`:
   - `processElement1` (clicks): read the tier from state, emit
     `EnrichedClick(click, tier ?: "unknown")`.
   - `processElement2` (profiles): update state, emit nothing.

**Bonus question:** When would `BroadcastState` be a better fit than
`connect + keyBy`?

> *Answer:* The dividing line is the **cardinality** of the reference stream.
> - `connect + keyBy` (this stage) — high cardinality (e.g. millions of
>   per-user profiles). State is partitioned across subtasks; no single TM
>   needs to hold the whole table.
> - `BroadcastState` — low cardinality (a small lookup table). Every subtask
>   gets the full table, and the main stream doesn't need to be re-shuffled
>   onto the lookup key. Typical use: feature flags, rule engines,
>   currency conversion tables, enum-style category metadata.
>
> Both are *soft joins* — the race between a click and its profile is
> inherent. Strict joins require buffering the click side until the profile
> arrives, or aligning the two streams via watermarks.

---

## When you're done

Run the full pipeline. You should see seven labelled streams interleaved:
`PURCHASE-TUPLE`, `REVENUE`, `ABANDONED`, `HIGH-VALUE`, `REGULAR`,
`NON-PURCHASE`, `ENRICHED`.

Move on to **Lesson 2 — Data Routing** once Stages 3 and 5 feel comfortable.
