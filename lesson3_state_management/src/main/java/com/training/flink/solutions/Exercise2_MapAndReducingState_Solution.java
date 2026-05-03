package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Exercise 2 — Solution: MapState + ReducingState
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise2_MapAndReducingState_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   MapState vs ValueState&lt;HashMap&gt;?
 *
 * ANSWER:
 *   MapState wins on:
 *     1. PARTIAL UPDATES. put(k,v) writes one row to the state backend.
 *        ValueState&lt;HashMap&gt; rewrites the WHOLE map every time, since the
 *        state backend sees the value as one opaque blob.
 *     2. ITERATION + PROJECTION. You can iterate keys, values, or entries
 *        without ever materialising the whole map. Useful when the map can
 *        get big (think: thousands of categories per user).
 *     3. RocksDB COMPACTION. Per-row entries can be compacted independently
 *        and benefit from key prefix scans, instead of a single value blob.
 *
 *   ValueState&lt;HashMap&gt; can still be the right call when:
 *     - the map is TINY and bounded (handful of entries) — the overhead of
 *       serializing a small map in one shot is lower than per-key state
 *       backend round trips.
 *     - you need atomic snapshot semantics across multiple keys (e.g. you
 *       want to see "all keys at time T or none"). MapState's iterators
 *       don't offer that guarantee under concurrent updates within a slot.
 * --------------------------------------------------------------------------
 */
public class Exercise2_MapAndReducingState_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "books", 12.0, 1000L),
                new ClickEvent("u1", "toys", 9.0, 1001L),
                new ClickEvent("u2", "electronics", 200.0, 1002L),
                new ClickEvent("u1", "books", 14.0, 1003L),
                new ClickEvent("u1", "electronics", 99.0, 1004L),
                new ClickEvent("u2", "books", 11.0, 1005L),
                new ClickEvent("u1", "books", 8.0, 1006L)
        );

        clicks
                .keyBy(c -> c.userId)
                .process(new SummarizePerUser())
                .print();

        env.execute("Exercise 2 - MapState + ReducingState (solution)");
    }

    public static class SummarizePerUser
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
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) throws Exception {
            // MapState: per-row update
            Long prev = catCounts.get(event.category);
            catCounts.put(event.category, (prev == null ? 0L : prev) + 1L);

            // ReducingState: just .add() — the reduce function does the math
            spend.add(event.price);

            // Read entries for output
            Map<String, Long> snapshot = new LinkedHashMap<>();
            for (Map.Entry<String, Long> e : catCounts.entries()) {
                snapshot.put(e.getKey(), e.getValue());
            }

            out.collect(String.format("%s | spend=%.2f | byCategory=%s",
                    event.userId, spend.get(), snapshot));
        }
    }
}
