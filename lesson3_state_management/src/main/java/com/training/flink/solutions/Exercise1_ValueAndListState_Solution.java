package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Exercise 1 — Solution: ValueState + ListState
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise1_ValueAndListState_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   Why is ListState a better fit than ValueState&lt;List&lt;String&gt;&gt; for "last N items"?
 *
 * ANSWER:
 *   1. APPEND IS O(1). With ListState.add(x), the state backend just appends
 *      a single serialized entry. With ValueState&lt;List&gt;, every update
 *      DESERIALIZES the whole list, mutates it, then RE-SERIALIZES the
 *      whole thing. On RocksDB, that's a full read-modify-write of a
 *      possibly multi-KB blob on every event.
 *
 *   2. RocksDB has a NATIVE list-merge operator. ListState.add() turns into
 *      a RocksDB Merge — no read on the write path at all. ValueState updates
 *      are always Get + Put.
 *
 *   3. Smaller checkpoints in incremental mode: list entries are stored
 *      separately, so unchanged tail entries don't have to be re-uploaded.
 *
 *   The downside is that ListState gives you no built-in size limit — you
 *   have to trim it yourself (we do that with .update(trimmed) below).
 * --------------------------------------------------------------------------
 */
public class Exercise1_ValueAndListState_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "books", 12.0, 1000L),
                new ClickEvent("u2", "toys", 8.0, 1001L),
                new ClickEvent("u1", "toys", 9.0, 1002L),
                new ClickEvent("u1", "books", 14.0, 1003L),
                new ClickEvent("u2", "electronics", 200.0, 1004L),
                new ClickEvent("u1", "electronics", 99.0, 1005L),
                new ClickEvent("u1", "clothing", 30.0, 1006L),
                new ClickEvent("u2", "books", 11.0, 1007L)
        );

        clicks
                .keyBy(c -> c.userId)
                .process(new EnrichWithHistory())
                .print();

        env.execute("Exercise 1 - ValueState + ListState (solution)");
    }

    public static class EnrichWithHistory
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
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) throws Exception {
            // 1. Bump the counter
            Long current = count.value();
            long next = (current == null ? 0L : current) + 1L;
            count.update(next);

            // 2. Append the new category and trim to the last MAX_RECENT
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
}
