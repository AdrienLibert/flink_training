package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.PacedClickSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reference solution for {@code com.training.flink.exercises.StatefulAnalytics}.
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.solutions.StatefulAnalytics_Solution"
 *
 * See EXERCISES.md for bonus-question answers.
 */
public class StatefulAnalytics_Solution {

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

        clickHistory(burstClicks).print("HISTORY");
        spendSummary(burstClicks).print("SPEND");
        sessionCounter(pacedClicks).print("SESSION");
        batchedFlush(burstClicks).print("BATCH");

        env.execute("Lesson 3 - Stateful analytics (solution)");
    }

    static DataStream<String> clickHistory(DataStream<ClickEvent> clicks) {
        return clicks.keyBy(c -> c.userId).process(new ClickHistoryEnricher());
    }

    static DataStream<String> spendSummary(DataStream<ClickEvent> clicks) {
        return clicks.keyBy(c -> c.userId).process(new SpendSummaryAggregator());
    }

    static DataStream<String> sessionCounter(DataStream<ClickEvent> clicks) {
        return clicks.keyBy(c -> c.userId).process(new TtlSessionCounter());
    }

    static DataStream<String> batchedFlush(DataStream<ClickEvent> clicks) {
        return clicks.map(new BufferedBatcher(3));
    }

    public static class ClickHistoryEnricher
            extends KeyedProcessFunction<String, ClickEvent, String> {

        private static final int MAX_RECENT = 5;

        private transient ValueState<Long> count;
        private transient ListState<String> recent;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("clickCount", Long.class));
            recent = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("recentCategories", String.class));
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out)
                throws Exception {
            Long current = count.value();
            long next = (current == null ? 0L : current) + 1L;
            count.update(next);

            recent.add(event.category);
            List<String> all = new ArrayList<>();
            recent.get().forEach(all::add);
            if (all.size() > MAX_RECENT) {
                all = all.subList(all.size() - MAX_RECENT, all.size());
                recent.update(all);
            }

            out.collect(String.format("%s | clicks=%d | recent=%s",
                    event.userId, next, all));
        }
    }

    public static class SpendSummaryAggregator
            extends KeyedProcessFunction<String, ClickEvent, String> {

        private transient MapState<String, Long> catCounts;
        private transient ReducingState<Double> spend;

        @Override
        public void open(Configuration parameters) {
            catCounts = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "categoryCounts",
                            TypeInformation.of(new TypeHint<String>() {}),
                            TypeInformation.of(new TypeHint<Long>() {})));

            spend = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<>(
                            "totalSpend",
                            (ReduceFunction<Double>) Double::sum,
                            Double.class));
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out)
                throws Exception {
            Long prev = catCounts.get(event.category);
            catCounts.put(event.category, (prev == null ? 0L : prev) + 1L);
            spend.add(event.price);

            Map<String, Long> snapshot = new LinkedHashMap<>();
            for (Map.Entry<String, Long> e : catCounts.entries()) {
                snapshot.put(e.getKey(), e.getValue());
            }

            out.collect(String.format("%s | spend=%.2f | byCategory=%s",
                    event.userId, spend.get(), snapshot));
        }
    }

    public static class TtlSessionCounter
            extends KeyedProcessFunction<String, ClickEvent, String> {

        private transient ValueState<Long> counter;
        private transient long lastSeenWallMs;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig
                    .newBuilder(Time.seconds(3))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupIncrementally(10, true)
                    .build();

            ValueStateDescriptor<Long> desc =
                    new ValueStateDescriptor<>("counter", Long.class);
            desc.enableTimeToLive(ttl);

            counter = getRuntimeContext().getState(desc);
            lastSeenWallMs = System.currentTimeMillis();
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out)
                throws Exception {
            long now = System.currentTimeMillis();
            long gap = now - lastSeenWallMs;
            lastSeenWallMs = now;

            Long current = counter.value();
            long next = (current == null ? 0L : current) + 1L;
            counter.update(next);

            out.collect(String.format("%s | gap=%dms | counter=%d %s",
                    event.userId, gap, next,
                    current == null ? "(fresh / expired)" : ""));
        }
    }

    public static class BufferedBatcher
            implements MapFunction<ClickEvent, String>, CheckpointedFunction {

        private final int batchSize;
        private final List<ClickEvent> localBuffer = new ArrayList<>();
        private transient ListState<ClickEvent> buffered;

        public BufferedBatcher(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public String map(ClickEvent value) {
            localBuffer.add(value);
            if (localBuffer.size() >= batchSize) {
                String flush = "FLUSH " + localBuffer.size() + " events: "
                        + localBuffer.stream()
                                .map(c -> c.category)
                                .collect(Collectors.joining(","));
                localBuffer.clear();
                return flush;
            }
            return "buffering: " + value.category + " (size=" + localBuffer.size() + ")";
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            buffered.update(new ArrayList<>(localBuffer));
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            ListStateDescriptor<ClickEvent> desc = new ListStateDescriptor<>(
                    "opBuffer",
                    TypeInformation.of(new TypeHint<ClickEvent>() {}));

            buffered = ctx.getOperatorStateStore().getListState(desc);

            if (ctx.isRestored()) {
                for (ClickEvent e : buffered.get()) {
                    localBuffer.add(e);
                }
            }
        }
    }
}
