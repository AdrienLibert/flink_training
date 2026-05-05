# Lesson 11 — Data Enrichment

Stream events come with IDs; downstream consumers want full records.
Joining one against the other is **enrichment**, and the wrong choice
of strategy is the most common cause of "our Flink job is slow" in
production.

Three strategies, in order of when to use them:

| Reference data shape | Tool | Why |
| --- | --- | --- |
| Slow-changing, small (config, country codes) | **Broadcast state** | Fan out once, read in-memory |
| Slow-changing, large but per-key (user profile) | **Async lookup** with cache | Avoid loading the whole DB |
| Versioned over time (FX rates, prices) | **Temporal table join** (SQL) | "What was the rate at 14:32?" |

This lesson focuses on **async lookups** — the workhorse for the
"per-event lookup" use case — with a quick theory section on the other
two.

## How this lesson works

Open `exercises/AsyncEnrichment.java` and implement:

1. `UserLookupAsyncFunction.asyncInvoke` — the per-element async call.
2. `enrich(...)` — wires the function into the pipeline via
   `AsyncDataStream.unorderedWait`.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.AsyncEnrichment"
```

Reference: `solutions/AsyncEnrichment_Solution.java`.

The fake "DB" is a `Map<String, String>` of 4 users; we sleep 100ms
inside the async function to simulate network latency. The proof that
async is working is the **output order** — with `unorderedWait`,
results emit as soon as each future completes, so the order won't
match the input order.

---

## Stage 1 — `asyncInvoke`

```java
@Override
public void asyncInvoke(String userId, ResultFuture<Tuple2<String, String>> resultFuture) {
    CompletableFuture
        .supplyAsync(() -> {
            Thread.sleep(100);
            return FAKE_DB.getOrDefault(userId, "?");
        }, executor)
        .thenAccept(name -> resultFuture.complete(
            Collections.singletonList(Tuple2.of(userId, name))));
}
```

**Three rules of `asyncInvoke`:**

1. **Never block the calling thread.** `asyncInvoke` runs on the
   operator thread. If you `Thread.sleep` directly, you've made the
   whole point moot. Use a separate executor or, better, a real
   non-blocking client (`HttpClient.sendAsync(...)`,
   reactive Cassandra driver).
2. **Always complete the `ResultFuture`** — even on failure. If you
   forget, the operator's pending-request slot stays occupied forever
   and eventually capacity fills up. (Use `resultFuture.completeExceptionally(...)`
   or call `complete(emptyList())`.)
3. **Return a Collection, not a single value.** A single async call may
   produce multiple output rows (e.g., a flat-map enrichment); the API
   accepts `Collection<OUT>`.

**Bonus question:** Why does `RichAsyncFunction` use `Configuration`
rather than `OpenContext` like newer Rich functions?

> *Answer:* `RichAsyncFunction.open(Configuration)` is the Flink 1.x
> shape that hasn't yet been migrated to the `OpenContext` API. Flink
> 1.18 still ships it. In 1.19+ both will likely coexist; in 2.0 the
> Configuration overload will be removed. For now, write
> `open(Configuration parameters)` and ignore the parameters.

---

## Stage 2 — `enrich`

```java
return AsyncDataStream.unorderedWait(
    userIds, new UserLookupAsyncFunction(),
    1, TimeUnit.SECONDS, /*capacity=*/4);
```

Four parameters that need conscious choice:

- **input stream** — the upstream DataStream.
- **AsyncFunction** — your implementation.
- **timeout** — how long to wait for the future. After this, `timeout()`
  is called instead of `asyncInvoke`'s completion. Pick 2–5× your p99
  call latency.
- **capacity** — max concurrent in-flight requests per parallel
  subtask. The crucial number. Too low → throughput-bound; too high →
  you DOS the upstream service.

`unorderedWait` vs `orderedWait`:
- **unordered** — emit results as soon as they complete. Lowest
  latency, but downstream sees out-of-order outputs.
- **ordered** — buffer until earlier requests complete. Maintains
  input order, costs more state. Use when downstream cares about order
  (e.g., feeds an event-time aggregation).

**Bonus question:** Why is the default `unorderedWait` for new code?

> *Answer:* Most enrichment pipelines feed downstream operators that
> use event-time, not arrival-order. The keyed/windowed operators
> deal with reordering correctly via watermarks. Insisting on
> `orderedWait` adds latency and state for no benefit. Use ordered only
> when your downstream genuinely cares about arrival order — e.g., a
> sink that writes to an append-only file you need to be in input
> order, or a CEP pattern that defines event order by arrival.

---

## Stage 3 — Theory: Broadcast state

When the reference data fits in memory and changes rarely (configs,
country codes, feature flags), broadcast state is faster and simpler:

```java
MapStateDescriptor<String, String> bcastDesc =
    new MapStateDescriptor<>("countries", Types.STRING, Types.STRING);

BroadcastStream<Tuple2<String,String>> bcast = countries.broadcast(bcastDesc);
events.connect(bcast)
      .process(new BroadcastProcessFunction<>() {
          public void processElement(Event e, ReadOnlyContext ctx, Collector<Out> out) {
              ReadOnlyBroadcastState<String,String> s = ctx.getBroadcastState(bcastDesc);
              out.collect(new Out(e, s.get(e.country)));
          }
          public void processBroadcastElement(Tuple2<String,String> kv, Context ctx, Collector<Out> out) {
              ctx.getBroadcastState(bcastDesc).put(kv.f0, kv.f1);
          }
      });
```

Pros: zero network calls per event. Cons: the broadcast data lives in
**every** parallel subtask's state — don't broadcast 100GB of user
records.

**Bonus question:** What invariant does `BroadcastProcessFunction`
preserve?

> *Answer:* All parallel subtasks see broadcast updates in the same
> order. That makes the `BroadcastState` on every subtask
> deterministically equivalent — no reasoning about "what if shard 3
> hasn't seen this update yet". The mechanism: Flink delivers each
> broadcast record to *all* downstream subtasks before any of them
> processes the next element on the broadcast side.

---

## Stage 4 — Theory: Temporal table joins (SQL)

For "what was the FX rate when this trade was placed", neither broadcast
nor async is right — you need point-in-time lookup. SQL handles this
natively:

```sql
SELECT t.trade_id, t.amount * fx.rate AS usd_amount
FROM trades AS t
JOIN fx_rates FOR SYSTEM_TIME AS OF t.event_time AS fx
  ON t.currency = fx.currency
```

Under the hood, the planner builds an event-time keyed state of the
fx_rates stream (versioned by event-time), and for every trade event
looks up the latest version with `event_time ≤ trade.event_time`.

**Bonus question:** Why is this hard to do in DataStream API?

> *Answer:* You'd need to maintain per-key state of "all versions of
> the rate, indexed by validity start", and on each trade event, do a
> reverse range scan to find the version that was current at the
> trade's event-time. Plus watermark coordination so you don't query
> in the future. The Table API does it for you with one SQL clause —
> this is one of the cases where SQL is genuinely easier than
> DataStream.

---

## When you're done

Output should look like (one line per event, plus the timing):

```
ENRICHED> (u0,Alice)
ENRICHED> (u1,Bob)
ENRICHED> (u2,Carol)
ENRICHED> (u3,Dave)
ENRICHED> (u0,Alice)
... (10 rows)
[timing] elapsed wall ms = ~300 (10 events × 100ms latency would be 1000ms+ if synchronous)
```

The order may differ from input — that's `unorderedWait` working.

Move on to **Lesson 12 — Data Skew**.
