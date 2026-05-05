package com.training.flink.solutions;

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

public class SkewLab_Solution {

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

        events(env)
                .map(new SaltedKeyBuilder())
                .keyBy(t -> t.f0)
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .map(new SubtaskCountReporter("V1_salted"))
                .addSink(new DiscardingSink<>());

        env.execute("V1 salted");
    }

    static DataStream<Tuple2<String, Long>> events(StreamExecutionEnvironment env) {
        return env.addSource(new SkewedSource(N))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));
    }

    public static class SaltedKeyBuilder
            extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private transient Random random;

        @Override
        public void open(org.apache.flink.configuration.Configuration p) {
            random = new Random();
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> in) {
            return Tuple2.of(in.f0 + "#" + random.nextInt(SALT_BUCKETS), in.f1);
        }
    }

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
