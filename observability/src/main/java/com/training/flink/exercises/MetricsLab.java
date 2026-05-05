package com.training.flink.exercises;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Random;

/**
 * Lesson 19 — Observability.
 *
 * Three metric primitives, each useful in production:
 *
 *   - Counter — monotonically increasing (records seen, errors, retries).
 *   - Gauge   — point-in-time value (queue depth, backlog, last seen ts).
 *   - Histogram — distribution (latency, payload size).
 *
 * In production these flow through `MetricGroup` to Prometheus, Datadog,
 * StatsD, etc. Here we print them at close() to verify the API is wired
 * correctly.
 *
 * Stage 1: increment {@link DroppedFilter#droppedCounter} on every
 *          dropped record.
 * Stage 2: register a {@link Gauge} {@code latestAmount} that returns
 *          the latest record's amount.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.MetricsLab"
 */
public class MetricsLab {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(generate(50))
                .map(new AmountTracker())
                .filter(new DroppedFilter(25.0))
                .addSink(new DiscardingSink<>());

        env.execute("Lesson 19 — Metrics");
    }

    static Double[] generate(int n) {
        Random r = new Random(42);
        Double[] out = new Double[n];
        for (int i = 0; i < n; i++) {
            out[i] = 5.0 + r.nextInt(50);
        }
        return out;
    }

    public static class AmountTracker extends RichMapFunction<Double, Double> {
        private transient double latest;

        @Override
        public void open(org.apache.flink.configuration.Configuration p) {
            // Stage 2: register a Gauge.
            // TODO: getRuntimeContext().getMetricGroup()
            //         .gauge("latestAmount", (Gauge<Double>) () -> latest);
            throw new UnsupportedOperationException("Implement AmountTracker.open (Stage 2)");
        }

        @Override
        public Double map(Double value) {
            latest = value;
            return value;
        }
    }

    public static class DroppedFilter extends RichFilterFunction<Double> {
        private final double threshold;
        private transient Counter droppedCounter;

        public DroppedFilter(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration p) {
            droppedCounter = getRuntimeContext().getMetricGroup()
                    .counter("droppedRecords");
        }

        @Override
        public boolean filter(Double value) {
            // Stage 1: if value < threshold, increment droppedCounter and return false.
            // Otherwise return true.
            // TODO
            throw new UnsupportedOperationException("Implement DroppedFilter.filter (Stage 1)");
        }

        @Override
        public void close() {
            // For demo visibility. In production these flow to a metric reporter.
            System.out.println("[DroppedFilter] dropped " + droppedCounter.getCount() + " records");
        }
    }
}
