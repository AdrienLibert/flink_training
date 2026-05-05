package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Lesson 2 — DataStream API: tour the partitioning strategies.
 *
 * One big exercise broken into 4 stages. Each stage builds a small subgraph
 * that prints how records flow across parallel subtasks. The four subgraphs
 * are submitted in a single job ({@link #main(String[])}) and their output
 * is interleaved with clear labels.
 *
 * Recommended order: Stage 1 → Stage 2 → Stage 3 → Stage 4.
 * Whichever stage you have not yet implemented will throw
 * {@link UnsupportedOperationException} when its subgraph is built.
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.RoutingTour"
 *
 * See {@code EXERCISES.md} for the per-stage spec, expected output, and
 * bonus questions.
 */
public class RoutingTour {

    public static final MapStateDescriptor<String, Double> MULTIPLIERS =
            new MapStateDescriptor<>("multipliers", Types.STRING, Types.DOUBLE);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        demoForwardRebalanceRescale(env);
        demoKeyByHotspot(env);
        demoBroadcast(env);
        demoCustomPartitioner(env);

        env.execute("Lesson 2 - Routing tour");
    }

    /**
     * Stage 1: contrast the three default partitioning strategies side-by-side.
     *
     * Skills: default forwarding, {@code .rebalance()}, {@code .rescale()}.
     *
     * Build three subgraphs from the same source:
     *   - default forward → {@code .map(new SubtaskTagger<>()).print("FORWARD")}
     *   - {@code .rebalance()} → tag → {@code .print("REBALANCE")}
     *   - {@code .rescale()} → tag → {@code .print("RESCALE")}
     *
     * Source is parallelism-1, downstream is parallelism-4. Observe how each
     * strategy distributes the 16 records across subtasks 0..3.
     */
    static void demoForwardRebalanceRescale(StreamExecutionEnvironment env) {
        DataStream<ClickEvent> source = env.fromElements(uniformEvents()).setParallelism(1);
        throw new UnsupportedOperationException(
                "Stage 1: print FORWARD / REBALANCE / RESCALE views of `source`");
    }

    /**
     * Stage 2: show data skew under {@code keyBy} and contrast with {@code rebalance}.
     *
     * Skills: keyBy hashing, hot-key skew, the cost of state co-location.
     *
     * Build two subgraphs from the same skewed source (90 records share key
     * "vip_user", 10 records split across u0..u9):
     *   - {@code .keyBy(e -> e.userId)} → tag → {@code .print("KEYBY")}
     *     → one subtask receives ~90 records.
     *   - {@code .rebalance()} → tag → {@code .print("REBALANCE")}
     *     → roughly 25-25-25-25 across subtasks.
     */
    static void demoKeyByHotspot(StreamExecutionEnvironment env) {
        DataStream<ClickEvent> events = env.fromElements(skewedEvents()).setParallelism(1);
        throw new UnsupportedOperationException(
                "Stage 2: print KEYBY (skewed) and REBALANCE (uniform) views of `events`");
    }

    /**
     * Stage 3: enrich a click stream with a small reference table replicated
     * to every subtask via broadcast state.
     *
     * Skills: {@code BroadcastStream}, {@code BroadcastProcessFunction},
     * {@code BroadcastState} read/write.
     *
     * Build:
     *   - {@code BroadcastStream<CategoryMultiplier> bcast = multipliers.broadcast(MULTIPLIERS);}
     *   - {@code clicks.connect(bcast).process(new MultiplierApplier()).print("BROADCAST");}
     *
     * Implement {@link MultiplierApplier} below: write into broadcast state on
     * the multiplier side, read it on the click side.
     */
    static void demoBroadcast(StreamExecutionEnvironment env) {
        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "books",       10.0, 1000L),
                new ClickEvent("u2", "electronics", 100.0, 2000L),
                new ClickEvent("u3", "clothing",    50.0, 3000L),
                new ClickEvent("u4", "books",       20.0, 4000L),
                new ClickEvent("u5", "electronics", 200.0, 5000L)
        ).setParallelism(1);

        DataStream<CategoryMultiplier> multipliers = env.fromElements(
                new CategoryMultiplier("books",       1.0),
                new CategoryMultiplier("electronics", 1.5),
                new CategoryMultiplier("clothing",    0.8)
        ).setParallelism(1);

        throw new UnsupportedOperationException(
                "Stage 3: broadcast `multipliers` and connect with `clicks` via MultiplierApplier");
    }

    /**
     * Stage 4: route records to subtasks by domain logic with a custom partitioner.
     *
     * Skills: {@link Partitioner}, {@code partitionCustom}, modulo-by-numPartitions.
     *
     * Bucket by price:
     *   price &lt;   50  → bucket 0 (cheap)
     *   price &lt;  200  → bucket 1 (mid)
     *   price &lt; 1000  → bucket 2 (premium)
     *   else            → bucket 3 (luxury)
     *
     * Build:
     *   {@code events.partitionCustom(new PriceBucketPartitioner(), e -> e.price)
     *           .map(new SubtaskTagger<>()).print("BUCKETED");}
     *
     * Implement {@link PriceBucketPartitioner} below — and ALWAYS return
     * {@code bucket % numPartitions} so the partitioner stays correct under
     * parallelism changes.
     */
    static void demoCustomPartitioner(StreamExecutionEnvironment env) {
        DataStream<ClickEvent> events = env.fromElements(pricedEvents()).setParallelism(1);
        throw new UnsupportedOperationException(
                "Stage 4: partitionCustom by price bucket, tag, print as BUCKETED");
    }

    private static ClickEvent[] uniformEvents() {
        ClickEvent[] events = new ClickEvent[16];
        String[] cats = {"books", "electronics", "clothing", "toys"};
        for (int i = 0; i < 16; i++) {
            events[i] = new ClickEvent("u" + i, cats[i % 4], 10.0 + i, 1000L + i);
        }
        return events;
    }

    private static ClickEvent[] skewedEvents() {
        ClickEvent[] events = new ClickEvent[100];
        for (int i = 0; i < 90; i++) {
            events[i] = new ClickEvent("vip_user", "electronics", 100.0, 1000L + i);
        }
        for (int i = 0; i < 10; i++) {
            events[90 + i] = new ClickEvent("u" + i, "books", 20.0, 2000L + i);
        }
        return events;
    }

    private static ClickEvent[] pricedEvents() {
        return new ClickEvent[]{
                new ClickEvent("u1", "books",       10.0,    1000L),  // bucket 0
                new ClickEvent("u2", "electronics", 75.0,    2000L),  // bucket 1
                new ClickEvent("u3", "electronics", 350.0,   3000L),  // bucket 2
                new ClickEvent("u4", "watches",     5_000.0, 4000L),  // bucket 3
                new ClickEvent("u5", "clothing",    25.0,    5000L),  // bucket 0
                new ClickEvent("u6", "electronics", 150.0,   6000L),  // bucket 1
                new ClickEvent("u7", "watches",     2_500.0, 7000L),  // bucket 3
                new ClickEvent("u8", "electronics", 800.0,   8000L)   // bucket 2
        };
    }

    /** Stage 3 reference-data type. */
    public static class CategoryMultiplier {
        public String category;
        public double multiplier;

        public CategoryMultiplier() {}

        public CategoryMultiplier(String category, double multiplier) {
            this.category = category;
            this.multiplier = multiplier;
        }
    }

    /**
     * Stage 3 helper. Implement processElement (read multiplier from broadcast
     * state) and processBroadcastElement (write the multiplier into state).
     */
    public static class MultiplierApplier
            extends BroadcastProcessFunction<ClickEvent, CategoryMultiplier, String> {

        @Override
        public void processElement(ClickEvent click,
                                   ReadOnlyContext ctx,
                                   Collector<String> out) throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 3: read multiplier for click.category and emit a formatted line");
        }

        @Override
        public void processBroadcastElement(CategoryMultiplier update,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 3: store update.multiplier in BroadcastState keyed by update.category");
        }
    }

    /** Stage 4 helper. Implement {@link Partitioner#partition(Object, int)}. */
    public static class PriceBucketPartitioner implements Partitioner<Double> {
        @Override
        public int partition(Double price, int numPartitions) {
            throw new UnsupportedOperationException(
                    "Stage 4: bucket the price and return bucket % numPartitions");
        }
    }
}
