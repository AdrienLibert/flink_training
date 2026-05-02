package com.training.flink.exercises;

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
 * Exercise 3: Broadcast — small reference stream replicated to ALL subtasks
 *
 * Goal:
 *   Use a BROADCAST stream to share a small set of "category multipliers"
 *   with every parallel subtask. Each ClickEvent's price is multiplied by
 *   the current multiplier for its category, producing a stream of
 *   (category, adjustedPrice) records.
 *
 *   Why this is interesting:
 *   - Click stream is keyed BY USERID (high cardinality, partitioned state).
 *   - Multiplier stream is keyed BY CATEGORY (low cardinality, ~5 entries).
 *   - We can't just connect+keyBy because the join keys are different.
 *   - Solution: broadcast the small stream so every subtask has the full
 *     multiplier table available locally.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise3_Broadcast"
 *
 * Bonus question:
 *   What is the cardinality threshold above which broadcast becomes a
 *   bad idea? (Hint: every subtask stores a FULL copy of the broadcast
 *   state in memory — multiply by parallelism.)
 */
public class Exercise3_Broadcast {

    /** Broadcast state descriptor — visible to ALL subtasks. */
    public static final MapStateDescriptor<String, Double> MULTIPLIERS =
            new MapStateDescriptor<>("multipliers", Types.STRING, Types.DOUBLE);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Main stream: a flow of click events
        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "books",       10.0, 1000L),
                new ClickEvent("u2", "electronics", 100.0, 2000L),
                new ClickEvent("u3", "clothing",    50.0, 3000L),
                new ClickEvent("u4", "books",       20.0, 4000L),
                new ClickEvent("u5", "electronics", 200.0, 5000L)
        ).setParallelism(1);

        // Reference stream: pricing multipliers per category
        DataStream<CategoryMultiplier> multipliers = env.fromElements(
                new CategoryMultiplier("books",       1.0),
                new CategoryMultiplier("electronics", 1.5),
                new CategoryMultiplier("clothing",    0.8)
        ).setParallelism(1);

        // TODO 1: turn the multipliers stream into a BROADCAST stream.
        //   BroadcastStream<CategoryMultiplier> broadcast = multipliers.broadcast(MULTIPLIERS);

        // TODO 2: connect clicks (a non-keyed DataStream) with broadcast,
        //   then call .process(...) with a BroadcastProcessFunction.
        //
        //   clicks
        //       .connect(broadcast)
        //       .process(new MultiplierApplier())
        //       .print();
        //
        //   Note: the click stream is NOT keyed in this example — see the
        //   parent class signature: BroadcastProcessFunction<IN1, IN2, OUT>.
        //   For a KEYED main stream, use KeyedBroadcastProcessFunction instead.

        env.execute("Exercise 3 - Broadcast");
    }

    /** A multiplier update: "books" -> 1.0, "electronics" -> 1.5, etc. */
    public static class CategoryMultiplier {
        public String category;
        public double multiplier;

        public CategoryMultiplier() {}

        public CategoryMultiplier(String category, double multiplier) {
            this.category = category;
            this.multiplier = multiplier;
        }
    }

    // TODO 3: implement a BroadcastProcessFunction<ClickEvent, CategoryMultiplier, String>
    //   - processElement: read the multiplier for this click's category from
    //                     ReadOnlyBroadcastState; emit "category=X price=Y * mult=Z = adjusted=W".
    //                     If no multiplier yet, emit "no multiplier" or skip.
    //   - processBroadcastElement: WRITE the (category, multiplier) into
    //                              BroadcastState (read-write here).
    //
    //   public static class MultiplierApplier
    //       extends BroadcastProcessFunction<ClickEvent, CategoryMultiplier, String> { ... }
}
