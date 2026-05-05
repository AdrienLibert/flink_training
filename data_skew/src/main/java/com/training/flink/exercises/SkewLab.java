package com.training.flink.exercises;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Lesson 12 — Data Skew.
 *
 * One key — let's call it "VIP" — gets 90% of the events. Naive
 * `keyBy(key)` sends all of them to the same subtask. That subtask is
 * 100% CPU; the other parallel subtasks are idle.
 *
 * V0 naive prints how many records each downstream subtask of the
 * reduce saw — the VIP subtask should be massively imbalanced.
 *
 * V1 salted maps the key to "key#salt" (small random suffix) before
 * `keyBy`, so VIP events are spread across SALT_BUCKETS partitions and
 * thus across all parallel subtasks.
 *
 * Stage 1: implement {@link SaltedKeyBuilder#map}.
 * Stage 2: wire it into {@link #runSalted}.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.SkewLab"
 */
public class SkewLab {

    private static final int PARALLELISM = 4;
    private static final int N = 4_000;
    private static final int SALT_BUCKETS = 8;

    public static void main(String[] args) throws Exception {
        runNaive();
        runSalted();
    }

    static void runNaive() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        events(env)
                .keyBy(t -> t.f0)
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .map(new SubtaskCountReporter("V0_naive"))
                .addSink(new DiscardingSink<>());

        env.execute("V0 naive");
    }

    static void runSalted() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        // TODO Stage 2: chain the salted pipeline:
        //   events(env)
        //     .map(new SaltedKeyBuilder())  // salt the key
        //     .keyBy(t -> t.f0)
        //     .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
        //     .map(new SubtaskCountReporter("V1_salted"))
        //     .addSink(new DiscardingSink<>());
        //
        // After this, you'd add a Phase 2 that strips the salt and
        // re-aggregates by original key — typically via a windowed
        // aggregation so phase 1 only emits 1 row per (key, window),
        // not 1 per input event. See EXERCISES.md.
        throw new UnsupportedOperationException("Implement runSalted");
    }

    static DataStream<Tuple2<String, Long>> events(StreamExecutionEnvironment env) {
        return env.addSource(new SkewedSource(N))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));
    }

    // ----- Stage 1 -----
    public static class SaltedKeyBuilder
            extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private transient Random random;

        @Override
        public void open(org.apache.flink.configuration.Configuration p) {
            random = new Random();
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> in) {
            // TODO: return Tuple2.of(in.f0 + "#" + random.nextInt(SALT_BUCKETS), in.f1)
            throw new UnsupportedOperationException("Implement SaltedKeyBuilder.map");
        }
    }

    /** Counts elements per parallel subtask, reports at close(). */
    public static class SubtaskCountReporter
            extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private static final AtomicLongArray COUNTS = new AtomicLongArray(64);
        private final String label;

        SubtaskCountReporter(String label) {
            this.label = label;
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> in) {
            COUNTS.incrementAndGet(getRuntimeContext().getIndexOfThisSubtask());
            return in;
        }

        @Override
        public void close() {
            int idx = getRuntimeContext().getIndexOfThisSubtask();
            long count = COUNTS.getAndSet(idx, 0L);
            if (count > 0) {
                System.out.printf("[%s] subtask %d saw %d records%n", label, idx, count);
            }
        }
    }

    public static class SkewedSource implements SourceFunction<Tuple2<String, Long>> {
        private final int n;
        private volatile boolean running = true;
        private final String[] minorKeys = {"u1", "u2", "u3"};

        SkewedSource(int n) {
            this.n = n;
        }

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) {
            Random r = new Random(42);
            for (int i = 0; i < n && running; i++) {
                String key = (r.nextInt(10) < 9) ? "VIP"
                        : minorKeys[r.nextInt(minorKeys.length)];
                ctx.collect(Tuple2.of(key, 1L));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
