package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
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
 * Exercise 3 — Solution: Broadcast (small reference data to all subtasks)
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise3_Broadcast_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   What's the cardinality threshold above which broadcast becomes a bad
 *   idea?
 *
 * ANSWER:
 *   There is no hard rule, but a useful frame:
 *
 *     total_memory_for_broadcast = entry_size * N_entries * parallelism
 *
 *   Every parallel subtask stores a FULL local copy of the broadcast state.
 *   At parallelism 100, a 10 MB broadcast becomes 1 GB of total cluster
 *   memory dedicated to one lookup table.
 *
 *   Pragmatic guidelines:
 *     - Up to ~10k entries / a few MB per subtask: broadcast is fine.
 *     - 10k–100k entries: still workable, but profile.
 *     - > 100k entries OR per-entry size grows over time: prefer
 *       connect+keyBy or an external lookup (e.g. async I/O to a key-value
 *       store) to avoid blowing memory.
 *
 *   Also consider:
 *     - Broadcast state is in heap memory (not RocksDB). It restores from
 *       checkpoints by reading the FULL state — large broadcasts make
 *       recovery slow.
 *     - Updates to broadcast state are sent to ALL subtasks, so update rate
 *       matters too. A frequently-updated 10MB table can saturate network.
 * --------------------------------------------------------------------------
 *
 * Note on stream type combinations:
 *   - DataStream + BroadcastStream  → BroadcastConnectedStream
 *       processed by:                 BroadcastProcessFunction
 *   - KeyedStream + BroadcastStream → BroadcastConnectedStream (keyed)
 *       processed by:                 KeyedBroadcastProcessFunction
 *
 *   This solution uses the non-keyed flavor.
 */
public class Exercise3_Broadcast_Solution {

    public static final MapStateDescriptor<String, Double> MULTIPLIERS =
            new MapStateDescriptor<>("multipliers", Types.STRING, Types.DOUBLE);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

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

        BroadcastStream<CategoryMultiplier> broadcast = multipliers.broadcast(MULTIPLIERS);

        clicks
                .connect(broadcast)
                .process(new MultiplierApplier())
                .print();

        env.execute("Exercise 3 - Broadcast (solution)");
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
}
