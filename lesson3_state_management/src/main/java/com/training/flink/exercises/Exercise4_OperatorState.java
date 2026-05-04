package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 4: Operator State (CheckpointedFunction)
 *
 * Goal:
 *   Build a NON-KEYED batching MapFunction. Records are buffered in memory
 *   and "flushed" (printed) every N events OR on checkpoint. To survive
 *   restarts, the buffer must be persisted via OPERATOR STATE — specifically
 *   ListState declared on the operator-state store.
 *
 *   Operator state is the right tool when there's no natural key — examples:
 *     - source offsets (Kafka consumer)
 *     - sink batches not yet flushed
 *     - per-subtask routing rules
 *
 * Concepts:
 *   - CheckpointedFunction has TWO methods:
 *       initializeState(ctx) — called on start AND on restore. Read state.
 *       snapshotState(ctx)   — called on every checkpoint. Write state.
 *   - getOperatorStateStore().getListState(desc) → "even-split" redistribution.
 *     On rescale, entries are spread evenly across new subtasks.
 *   - getOperatorStateStore().getUnionListState(desc) → "union" redistribution.
 *     Every subtask gets the FULL list. Useful for global config / Kafka
 *     offset tables small enough to broadcast.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise4_OperatorState"
 *
 * Bonus question:
 *   When would you choose union list state over the regular even-split
 *   list state, and what is the danger of using union list state for
 *   "buffered records"?
 */
public class Exercise4_OperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // Enable checkpointing so snapshotState() actually fires.
        env.enableCheckpointing(2_000);

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "books", 10.0, 1L),
                new ClickEvent("u2", "toys", 8.0, 2L),
                new ClickEvent("u3", "books", 12.0, 3L),
                new ClickEvent("u4", "electronics", 99.0, 4L),
                new ClickEvent("u5", "clothing", 30.0, 5L),
                new ClickEvent("u6", "books", 11.0, 6L),
                new ClickEvent("u7", "toys", 9.0, 7L)
        );

        clicks
                .map(new BatchingMapper(3))   // flush every 3 events per subtask
                .print();

        env.execute("Exercise 4 - Operator State");
    }

    /**
     * TODO: Make this class a {@link CheckpointedFunction}.
     *
     *  1. Add fields:
     *       private final int batchSize;
     *       private transient ListState&lt;ClickEvent&gt; buffered;
     *       private final List&lt;ClickEvent&gt; localBuffer = new ArrayList&lt;&gt;();
     *
     *  2. map(ClickEvent):
     *       - add value to localBuffer
     *       - if size reaches batchSize, build a "FLUSH: ..." string,
     *         clear the buffer, and return it
     *       - otherwise return "buffering: <category>" so you can see it accumulate
     *
     *  3. snapshotState(ctx):
     *       - buffered.update(localBuffer)   // overwrite the persisted snapshot
     *
     *  4. initializeState(ctx):
     *       - register a ListStateDescriptor&lt;ClickEvent&gt;("opBuffer")
     *       - buffered = ctx.getOperatorStateStore().getListState(desc);
     *       - if ctx.isRestored(): copy entries from buffered into localBuffer
     */
    public static class BatchingMapper implements MapFunction<ClickEvent, String> {

        private final int batchSize;

        public BatchingMapper(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public String map(ClickEvent value) {
            // TODO replace this placeholder with real batching + operator state
            return "passthrough: " + value.category;
        }
    }
}
