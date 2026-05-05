# Lesson 19 — Observability

You can't fix what you can't see. Flink ships with:

- A **MetricGroup** API for emitting custom metrics (Counter, Gauge,
  Histogram, Meter).
- Built-in metrics for every operator (`numRecordsIn`,
  `numBytesOut`, `currentInputWatermark`, etc.).
- A pluggable **MetricReporter** that ships these to Prometheus,
  Datadog, StatsD, JMX, or anywhere you need.

This lesson exercises the API and walks through the production
patterns.

## How this lesson works

Open `exercises/MetricsLab.java` and implement two metric registrations.

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.MetricsLab"
```

Reference: `solutions/MetricsLab_Solution.java`.

In a real cluster, the metrics flow to Prometheus. Here we just print
them at `close()` to verify the API.

---

## Stage 1 — Counter (`droppedCounter`)

```java
@Override
public void open(Configuration p) {
    droppedCounter = getRuntimeContext().getMetricGroup()
        .counter("droppedRecords");
}

@Override
public boolean filter(Double value) {
    if (value < threshold) {
        droppedCounter.inc();
        return false;
    }
    return true;
}
```

The metric ID in Prometheus would be:
`flink_taskmanager_job_task_operator_droppedRecords`. The path is
auto-derived from job name → operator name → user-given metric name.

**Bonus question:** Why use Counter instead of a regular `int`?

> *Answer:* The MetricReporter polls counters atomically (and resets,
> for some reporters). A user-private `int` would lose updates between
> threads, would not be exported, and you'd be guessing every time the
> dashboard didn't move whether it was the metric or your code that
> broke. The Counter contract is "this is exposed; the system reads
> it for you".

---

## Stage 2 — Gauge (`latestAmount`)

```java
@Override
public void open(Configuration p) {
    getRuntimeContext().getMetricGroup()
        .gauge("latestAmount", (Gauge<Double>) () -> latest);
}

@Override
public Double map(Double value) {
    latest = value;
    return value;
}
```

Two key differences vs Counter:

1. **Read by callback.** The reporter calls your `Gauge` lambda when
   it scrapes; you don't push values. So your gauge can compute a
   value on demand (queue depth, current backlog).
2. **Not aggregated.** Counters are summed across parallel subtasks
   into one cluster-level counter; gauges are kept per-subtask. If you
   want an aggregated gauge, register one yourself per metric scope.

**Bonus question:** What happens if your gauge lambda blocks for 10
seconds?

> *Answer:* The MetricReporter calls gauge lambdas on the metric thread.
> A blocking gauge stalls the entire metric scrape, missing the next
> scrape interval and triggering Prometheus stale-data warnings. Always
> make gauges fast — read a volatile field, not a state backend or DB.

---

## Stage 3 — Histogram

For latency, payload size, etc., use a Histogram:

```java
import org.apache.flink.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;

private transient Histogram latencyHist;

@Override
public void open(Configuration p) {
    com.codahale.metrics.Histogram dw = new com.codahale.metrics.Histogram(
        new SlidingWindowReservoir(1024));
    latencyHist = getRuntimeContext().getMetricGroup()
        .histogram("processLatencyMs", new DropwizardHistogramWrapper(dw));
}

@Override
public Double map(Double value) {
    long start = System.nanoTime();
    Double result = doWork(value);
    latencyHist.update((System.nanoTime() - start) / 1_000_000);
    return result;
}
```

This adds the `flink-metrics-dropwizard` dep. Or use Flink's built-in
`org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram` for
a simpler alternative without Dropwizard.

**Bonus question:** Why is histogram support gated behind Dropwizard?

> *Answer:* Histograms need a reservoir algorithm — sliding-window,
> exponentially-decaying, etc. Reservoir choice has serious
> implications for memory and accuracy. Flink stays out of that
> debate by accepting any Histogram implementation; Dropwizard
> supplies the most common ones. For most production cases use
> SlidingWindowReservoir(1024) or ExponentiallyDecayingReservoir.

---

## Stage 4 — Built-in metrics worth knowing

Every Flink operator emits these without any code:

| Metric | What | Why it matters |
| --- | --- | --- |
| `numRecordsInPerSecond` | Throughput in | The first answer to "how fast?" |
| `numRecordsOutPerSecond` | Throughput out | If in > out + buffered, backpressure |
| `currentInputWatermark` | Lowest input watermark | Lateness vs realtime |
| `currentOutputWatermark` | Watermark progress | If stuck, watermark generator broken |
| `busyTimeMsPerSecond` | Time spent processing | If close to 1000, operator is CPU-bound |
| `backPressuredTimeMsPerSecond` | Time blocked on output | If high, downstream too slow |
| `lastCheckpointDuration` | ms last checkpoint took | If approaching interval, tune state |
| `lastCheckpointSize` | bytes | Watch for unbounded growth |

**Bonus question:** What's the difference between `busyTime` and
`100 - idleTime`?

> *Answer:* They measure complementary things. `busy` is "time the
> operator was actively processing records". `idle` is "time the
> operator had no records to process". Their sum can be < 1000ms if
> the operator was *neither* busy nor idle — it was BLOCKED on a
> downstream backpressure signal or waiting on a checkpoint. The
> difference is `backPressuredTimeMsPerSecond`. Together: `busy +
> idle + backpressured = 1000ms` (per second).

---

## Stage 5 — Reporter config

Production reporter setup in `flink-conf.yaml`:

```yaml
metrics.reporters: prom
metrics.reporter.prom.factory.class: \
    org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
metrics.reporter.prom.host: 0.0.0.0
# Optionally filter:
# metrics.reporter.prom.filter.includes: '*:*:*:numRecordsIn*'
```

Add a Prometheus scrape config pointing at the JM/TM pods on port
9249. The metric names follow the pattern
`flink_<scope>_<scope>_..._<metric>`.

---

## When you're done

Run the job. You'll see:

```
[AmountTracker] latestAmount gauge = ...
[DroppedFilter] dropped XX records
```

Move on to **Lesson 20 — Building a Control Plane**.
