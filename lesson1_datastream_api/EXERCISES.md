# Lesson 1: Mastering DataStream API — Exercises

## Setup

You'll need:
- Java 11+
- Maven or Gradle
- Apache Flink 1.18+ dependencies

Add to your `pom.xml`:
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>1.18.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>1.18.1</version>
    </dependency>
</dependencies>
```

---

## Domain: E-commerce Click Stream

You're processing a stream of `ClickEvent` objects:

```java
public class ClickEvent {
    public String userId;
    public String productId;
    public String category;     // "electronics", "books", "clothing"
    public double price;
    public long timestamp;      // event time in millis
    public String action;       // "view", "add_to_cart", "purchase"
}
```

---

## Exercise 1: Basic Transformations (Warm-up)

**Goal:** Build a pipeline that:
1. Reads from a source of `ClickEvent` (use `env.fromElements(...)` with sample data)
2. Filters only `"purchase"` actions
3. Maps each purchase to a `(category, price)` tuple
4. Prints the result

**Skills:** `filter`, `map`, basic source/sink

**File to create:** `Exercise1_BasicPipeline.java`

---

## Exercise 2: KeyBy + Aggregation

**Goal:** Compute the **total revenue per category** in real time.

Requirements:
- Key by `category`
- Use `reduce` or `sum` to accumulate revenue
- Print updates as they happen (each new purchase emits an updated total)

**Skills:** `keyBy`, `reduce`, stateful aggregation

**File to create:** `Exercise2_RevenuePerCategory.java`

**Question to answer in a comment:** Why does the output show *multiple* totals for the same category instead of a single final number?

---

## Exercise 3: ProcessFunction with State

**Goal:** Detect users who add an item to their cart but never purchase it within **30 seconds**.

Requirements:
- Use a `KeyedProcessFunction<String, ClickEvent, Alert>` keyed by `userId`
- Use `ValueState<Long>` to remember the last `add_to_cart` timestamp
- Use a **processing-time timer** to fire after 30s
- If a `purchase` arrives before the timer fires, clear the state
- Otherwise, emit an `Alert(userId, "abandoned cart")`

**Skills:** `KeyedProcessFunction`, `ValueState`, timers, state clearing

**File to create:** `Exercise3_AbandonedCart.java`

**Bonus:** Switch from processing-time to event-time timers. What changes about the watermark requirements?

---

## Exercise 4: Side Outputs & Routing

**Goal:** Split the stream into three separate outputs:
- High-value purchases (`price > 500`) → main output
- Regular purchases → side output `regular`
- All non-purchase events → side output `non-purchase`

Requirements:
- Use `OutputTag` and a `ProcessFunction`
- Print each side output with a clear label

**Skills:** `OutputTag`, `ctx.output()`, stream routing

**File to create:** `Exercise4_SideOutputs.java`

---

## Exercise 5: Connect Two Streams

**Goal:** Enrich `ClickEvent`s with a slowly-changing `UserProfile` stream.

Given a second stream:
```java
public class UserProfile {
    public String userId;
    public String tier;        // "free", "premium", "vip"
}
```

Requirements:
- `connect` the two streams keyed by `userId`
- Use `KeyedCoProcessFunction` to store the latest profile per user in state
- For each click, emit `EnrichedClick(click, tier)` — use `"unknown"` if profile not yet seen

**Skills:** `connect`, `KeyedCoProcessFunction`, broadcast vs. keyed enrichment

**File to create:** `Exercise5_StreamEnrichment.java`

**Question:** When would you use `BroadcastState` instead of `connect`+`keyBy`? (Hint: think about cardinality of the slow stream.)

---

## Submission / Self-check

For each exercise:
1. Run locally with `env.execute()`.
2. Verify output matches the expected behavior.
3. Note any surprises in a `NOTES.md` — those are the most important learning moments.

Move on to **Lesson 2 (Data Routing)** once Exercises 3 and 5 feel comfortable — they're the foundation for everything that follows.
