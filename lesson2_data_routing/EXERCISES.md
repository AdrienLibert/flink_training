# Lesson 2 — DataStream API: Data Routing

This lesson is about **how records move between operators**. In Flink, the
choice of partitioning strategy affects parallelism, ordering, latency,
state distribution, and skew. Picking the wrong one is one of the most
common production performance bugs.

## Domain

We'll keep using `ClickEvent` (a slightly simplified version lives in the
`model/` package of this lesson). All four exercises print **`subtask_id ⇒ record`**
so you can *see* where data goes.

## Exercises

| # | Title | Concept |
|---|-------|---------|
| 1 | Forward vs Rebalance | default forward partitioning, `.rebalance()`, `.rescale()` |
| 2 | KeyBy under Skew | hash partitioning + skewed keys → hotspots |
| 3 | Broadcast | the small-reference-stream pattern |
| 4 | Custom Partitioner | writing a `Partitioner<T>` for domain-specific routing |

## How to run

```bash
cd lesson2_data_routing
mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise1_ForwardVsRebalance"
```

Replace the class path with whichever exercise (or its `_Solution` under
`com.training.flink.solutions...`) you want to run.

## What to look for

Each exercise prints output like `subtask=3 | userId=u17 ...`. Your job is to
**match the observed distribution to the partitioning strategy** and
explain why. Solutions in `solutions/` include answered bonus questions.
