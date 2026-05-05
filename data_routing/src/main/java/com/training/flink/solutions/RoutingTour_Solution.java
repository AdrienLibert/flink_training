package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Reference solution for {@code com.training.flink.exercises.RoutingTour}.
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.solutions.RoutingTour_Solution"
 *
 * See EXERCISES.md for bonus-question answers.
 */
public class RoutingTour_Solution {

    public static final MapStateDescriptor<String, Double> MULTIPLIERS =
            new MapStateDescriptor<>("multipliers", Types.STRING, Types.DOUBLE);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        demoForwardRebalanceRescale(env);
        demoKeyByHotspot(env);
        demoBroadcast(env);
        demoCustomPartitioner(env);

        env.execute("Lesson 2 - Routing tour (solution)");
    }

    static void demoForwardRebalanceRescale(StreamExecutionEnvironment env) {
        DataStream<ClickEvent> source = env.fromElements(uniformEvents()).setParallelism(1);
        source.map(new SubtaskTagger<>()).print("FORWARD");
        source.rebalance().map(new SubtaskTagger<>()).print("REBALANCE");
        source.rescale().map(new SubtaskTagger<>()).print("RESCALE");
    }

    static void demoKeyByHotspot(StreamExecutionEnvironment env) {
        DataStream<ClickEvent> events = env.fromElements(skewedEvents()).setParallelism(1);
        events.keyBy(e -> e.userId).map(new SubtaskTagger<>()).print("KEYBY");
        events.rebalance().map(new SubtaskTagger<>()).print("REBALANCE");
    }

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

        BroadcastStream<CategoryMultiplier> bcast = multipliers.broadcast(MULTIPLIERS);
        clicks.connect(bcast).process(new MultiplierApplier()).print("BROADCAST");
    }

    static void demoCustomPartitioner(StreamExecutionEnvironment env) {
        DataStream<ClickEvent> events = env.fromElements(pricedEvents()).setParallelism(1);
        events
                .partitionCustom(new PriceBucketPartitioner(), e -> e.price)
                .map(new SubtaskTagger<>())
                .print("BUCKETED");
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
                new ClickEvent("u1", "books",       10.0,    1000L),
                new ClickEvent("u2", "electronics", 75.0,    2000L),
                new ClickEvent("u3", "electronics", 350.0,   3000L),
                new ClickEvent("u4", "watches",     5_000.0, 4000L),
                new ClickEvent("u5", "clothing",    25.0,    5000L),
                new ClickEvent("u6", "electronics", 150.0,   6000L),
                new ClickEvent("u7", "watches",     2_500.0, 7000L),
                new ClickEvent("u8", "electronics", 800.0,   8000L)
        };
    }

    public static class CategoryMultiplier {
        public String category;
        public double multiplier;

        public CategoryMultiplier() {}

        public CategoryMultiplier(String category, double multiplier) {
            this.category = category;
            this.multiplier = multiplier;
        }
    }

    public static class MultiplierApplier
            extends BroadcastProcessFunction<ClickEvent, CategoryMultiplier, String> {

        @Override
        public void processElement(ClickEvent click,
                                   ReadOnlyContext ctx,
                                   Collector<String> out) throws Exception {
            ReadOnlyBroadcastState<String, Double> state = ctx.getBroadcastState(MULTIPLIERS);
            Double mult = state.get(click.category);
            if (mult == null) {
                out.collect("(no multiplier yet) " + click);
                return;
            }
            double adjusted = click.price * mult;
            out.collect(String.format(
                    "category=%s | price=%.2f * mult=%.2f = adjusted=%.2f",
                    click.category, click.price, mult, adjusted));
        }

        @Override
        public void processBroadcastElement(CategoryMultiplier update,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
            BroadcastState<String, Double> state = ctx.getBroadcastState(MULTIPLIERS);
            state.put(update.category, update.multiplier);
        }
    }

    public static class PriceBucketPartitioner implements Partitioner<Double> {
        @Override
        public int partition(Double price, int numPartitions) {
            int bucket;
            if (price < 50.0)        bucket = 0;
            else if (price < 200.0)  bucket = 1;
            else if (price < 1000.0) bucket = 2;
            else                     bucket = 3;
            return bucket % numPartitions;
        }
    }
}
