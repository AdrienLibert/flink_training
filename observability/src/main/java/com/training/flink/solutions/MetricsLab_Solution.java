package com.training.flink.solutions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Random;

public class MetricsLab_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(generate(50))
                .map(new AmountTracker())
                .filter(new DroppedFilter(25.0))
                .addSink(new DiscardingSink<>());

        env.execute("Lesson 19 — Metrics (solution)");
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
            getRuntimeContext().getMetricGroup()
                    .gauge("latestAmount", (Gauge<Double>) () -> latest);
        }

        @Override
        public Double map(Double value) {
            latest = value;
            return value;
        }

        @Override
        public void close() {
            System.out.println("[AmountTracker] latestAmount gauge = " + latest);
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
            if (value < threshold) {
                droppedCounter.inc();
                return false;
            }
            return true;
        }

        @Override
        public void close() {
            System.out.println("[DroppedFilter] dropped " + droppedCounter.getCount() + " records");
        }
    }
}
