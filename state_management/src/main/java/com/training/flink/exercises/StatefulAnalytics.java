package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.PacedClickSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Lesson 3 — DataStream API: a single stateful per-user analytics pipeline
 * that exercises all four DataStream state types in one job.
 *
 * One big exercise broken into 4 stages. Each stage is a static method that
 * takes a click stream and returns a {@code DataStream<String>} of human-
 * readable output. {@link #main(String[])} wires them together and prints
 * each under a distinct label.
 *
 * Recommended order: Stage 1 → Stage 2 → Stage 3 → Stage 4. The class
 * compiles and {@code main()} runs as-is; whichever stage you have not yet
 * implemented will throw {@link UnsupportedOperationException} when its
 * subgraph is built (or its first record is processed).
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.StatefulAnalytics"
 *
 * See {@code EXERCISES.md} for per-stage spec, expected output, and bonus
 * questions (including the schema-evolution and State Processor API theory).
 */
public class StatefulAnalytics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(2_000);

        DataStream<ClickEvent> burstClicks = env.fromElements(
                new ClickEvent("u1", "books",       12.0, 1000L),
                new ClickEvent("u2", "toys",         8.0, 1001L),
                new ClickEvent("u1", "toys",         9.0, 1002L),
                new ClickEvent("u1", "books",       14.0, 1003L),
                new ClickEvent("u2", "electronics", 200.0, 1004L),
                new ClickEvent("u1", "electronics",  99.0, 1005L),
                new ClickEvent("u1", "clothing",    30.0, 1006L),
                new ClickEvent("u2", "books",       11.0, 1007L)
        );

        DataStream<ClickEvent> pacedClicks = env.addSource(new PacedClickSource(
                new ClickEvent[]{
                        new ClickEvent("u1", "books", 10.0, 1L),
                        new ClickEvent("u1", "books", 11.0, 2L),
                        new ClickEvent("u1", "books", 12.0, 3L),
                        new ClickEvent("u1", "books", 13.0, 4L)
                },
                new long[]{500, 1000, 5000, 1000}
        ));

        DataStream<String> history = clickHistory(burstClicks);
        DataStream<String> spend = spendSummary(burstClicks);
        DataStream<String> session = sessionCounter(pacedClicks);
        DataStream<String> batched = batchedFlush(burstClicks);

        history.print("HISTORY");
        spend.print("SPEND");
        session.print("SESSION");
        batched.print("BATCH");

        env.execute("Lesson 3 - Stateful analytics");
    }

    /**
     * Stage 1: per-user click count (ValueState) + last 5 categories (ListState).
     *
     * Skills: {@code ValueState<Long>}, {@code ListState<String>}, state
     * descriptors, manual list trimming.
     *
     * Hint: keyBy(userId) and apply a KeyedProcessFunction that holds both
     * pieces of state. Implement {@link ClickHistoryEnricher} below.
     *
     * Bonus question: why is ListState a better fit than ValueState&lt;List&gt;
     * for "last N items"? (Answer in EXERCISES.md.)
     */
    static DataStream<String> clickHistory(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 1: keyBy(userId).process(new ClickHistoryEnricher())");
    }

    /**
     * Stage 2: per-user category counts (MapState) + running spend (ReducingState).
     *
     * Skills: {@code MapState<String, Long>}, {@code ReducingState<Double>},
     * {@code ReduceFunction}.
     *
     * Hint: keyBy(userId) and apply a KeyedProcessFunction. Implement
     * {@link SpendSummaryAggregator} below.
     *
     * Bonus question: when would you still pick ValueState&lt;HashMap&gt; over
     * MapState? (Answer in EXERCISES.md.)
     */
    static DataStream<String> spendSummary(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 2: keyBy(userId).process(new SpendSummaryAggregator())");
    }

    /**
     * Stage 3: per-user session counter that resets when state expires (TTL=3s).
     *
     * Skills: {@code StateTtlConfig}, {@code UpdateType}, {@code StateVisibility},
     * cleanup strategies, processing-time observability via {@link PacedClickSource}.
     *
     * Hint: keyBy(userId) and apply {@link TtlSessionCounter} below. The paced
     * source emits 4 clicks for u1 with delays {500, 1000, 5000, 1000}ms — the
     * 3rd click should reset the counter to 1 because the gap exceeds the TTL.
     *
     * Bonus question: why is lazy (read-time) cleanup a production problem?
     * What other cleanup strategies does {@code StateTtlConfig} expose?
     * (Answer in EXERCISES.md.)
     */
    static DataStream<String> sessionCounter(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 3: keyBy(userId).process(new TtlSessionCounter())");
    }

    /**
     * Stage 4: non-keyed batching mapper backed by operator {@code ListState}.
     *
     * Skills: {@code CheckpointedFunction}, {@code OperatorStateStore},
     * even-split list state, restore semantics.
     *
     * Hint: just call {@code .map(new BufferedBatcher(3))} — implement the
     * class below. With the default sample data (7 events) you should see
     * one or two FLUSH lines and several "buffering" lines.
     *
     * Bonus question: when would union list state be the right call instead
     * of even-split, and what is the danger of using it for buffered records?
     * (Answer in EXERCISES.md.)
     */
    static DataStream<String> batchedFlush(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 4: clicks.map(new BufferedBatcher(3))");
    }

    /** Stage 1 helper. */
    public static class ClickHistoryEnricher
            extends KeyedProcessFunction<String, ClickEvent, String> {
        // TODO Stage 1:
        //   - declare transient ValueState<Long> count
        //   - declare transient ListState<String> recent
        //   - override open(Configuration) to initialize both descriptors
        //   - implement processElement: bump counter, append+trim recent (max 5),
        //     emit "userId | clicks=N | recent=[...]"

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out)
                throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 1: implement ClickHistoryEnricher.processElement()");
        }
    }

    /** Stage 2 helper. */
    public static class SpendSummaryAggregator
            extends KeyedProcessFunction<String, ClickEvent, String> {
        // TODO Stage 2:
        //   - declare transient MapState<String, Long> catCounts
        //   - declare transient ReducingState<Double> spend (ReduceFunction = Double::sum)
        //   - override open(Configuration) to initialize both descriptors
        //   - implement processElement: bump category count, spend.add(price),
        //     emit "userId | spend=… | byCategory={…}"

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out)
                throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 2: implement SpendSummaryAggregator.processElement()");
        }
    }

    /** Stage 3 helper. */
    public static class TtlSessionCounter
            extends KeyedProcessFunction<String, ClickEvent, String> {
        // TODO Stage 3:
        //   - build StateTtlConfig.newBuilder(Time.seconds(3)) with
        //       UpdateType.OnCreateAndWrite, StateVisibility.NeverReturnExpired,
        //       and an explicit cleanup strategy (e.g. cleanupIncrementally(10, true))
        //   - call enableTimeToLive(ttl) on the ValueStateDescriptor BEFORE
        //     handing it to getRuntimeContext().getState(...)
        //   - implement processElement: read counter (null = expired/fresh),
        //     bump it, emit "userId | gap=…ms | counter=…"

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out)
                throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 3: implement TtlSessionCounter.processElement()");
        }
    }

    /** Stage 4 helper. Make this implement {@link CheckpointedFunction}. */
    public static class BufferedBatcher implements MapFunction<ClickEvent, String>, CheckpointedFunction {

        private final int batchSize;
        // TODO Stage 4:
        //   - private transient ListState<ClickEvent> buffered
        //   - private final List<ClickEvent> localBuffer = new ArrayList<>()
        //   - in map(value): add to localBuffer, if size >= batchSize emit FLUSH
        //                    (clear buffer); else emit "buffering: <category>"
        //   - in snapshotState(ctx): buffered.update(new ArrayList<>(localBuffer))
        //   - in initializeState(ctx):
        //         register a ListStateDescriptor<ClickEvent>("opBuffer")
        //         buffered = ctx.getOperatorStateStore().getListState(desc)
        //         if (ctx.isRestored()) replay entries into localBuffer

        public BufferedBatcher(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public String map(ClickEvent value) {
            throw new UnsupportedOperationException(
                    "Stage 4: implement BufferedBatcher.map() with batched flush");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 4: implement BufferedBatcher.snapshotState() to persist localBuffer");
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 4: implement BufferedBatcher.initializeState() and restore buffer");
        }
    }
}
