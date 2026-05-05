package com.training.flink.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Lesson 22 — Benchmarking & Profiling.
 *
 * A throughput micro-benchmark for a stateless transform. Runs N
 * records through `map → discard` and prints records-per-second.
 *
 * Run with default config:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.Microbench"
 *
 * Then re-run with object reuse on. The throughput should rise:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.Microbench" \
 *       -Dexec.args="--reuse"
 *
 * Run with async profiler attached for a flame graph:
 *   ./scripts/profile-with-async-profiler.sh com.training.flink.exercises.Microbench
 */
public class Microbench {

    private static final int N = 5_000_000;

    public static void main(String[] args) throws Exception {
        boolean reuse = args.length > 0 && args[0].equals("--reuse");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        if (reuse) {
            env.getConfig().enableObjectReuse();
        }

        long start = System.currentTimeMillis();
        env.addSource(new BoundedLongSource(N))
                .map(x -> x + 1)
                .filter(x -> x % 7 != 0)
                .map(x -> Long.toString(x))
                .addSink(new DiscardingSink<>());

        env.execute("Microbench reuse=" + reuse);
        long elapsed = System.currentTimeMillis() - start;

        System.out.printf(
                "[bench] reuse=%b N=%d wall=%dms throughput=%,.0f recs/s%n",
                reuse, N, elapsed, (double) N / (elapsed / 1000.0));
    }

    public static class BoundedLongSource implements SourceFunction<Long> {
        private final long n;
        private volatile boolean running = true;

        BoundedLongSource(long n) {
            this.n = n;
        }

        @Override
        public void run(SourceContext<Long> ctx) {
            for (long i = 0; i < n && running; i++) {
                ctx.collect(i);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
