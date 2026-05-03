package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Exercise 1: ValueState + ListState
 *
 * Goal:
 *   Build a per-user enrichment that tracks:
 *     - a running CLICK COUNT (ValueState&lt;Long&gt;)
 *     - the LAST 5 categories clicked (ListState&lt;String&gt;)
 *
 *   For every incoming ClickEvent, output a string like:
 *     "u1 | clicks=4 | recent=[books, toys, books, electronics]"
 *
 * Concepts:
 *   - ValueState: a single value per key. Read with .value(), write with .update().
 *   - ListState : an append-only list per key. Use .add() / .get() / .update(List).
 *   - State descriptors are declared in open() so Flink can wire them to the
 *     state backend (memory / RocksDB) and recover them on restart.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise1_ValueAndListState"
 *
 * Bonus question (answer in a comment):
 *   Why is ListState a better fit than ValueState&lt;List&lt;String&gt;&gt; for the
 *   "last N items" use case? Think about (a) the RocksDB state backend, and
 *   (b) what happens on every .add() vs every .update(largerList).
 */
public class Exercise1_ValueAndListState {

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

        env.execute("Exercise 1 - ValueState + ListState");
    }

    /**
     * TODO: Implement the KeyedProcessFunction.
     *
     *  1. Declare two state DESCRIPTORS as fields of the function:
     *       ValueStateDescriptor<Long>   countDesc
     *       ListStateDescriptor<String>  recentDesc   (size 5)
     *
     *  2. In open(), initialise the corresponding state HANDLES:
     *       count  = getRuntimeContext().getState(countDesc);
     *       recent = getRuntimeContext().getListState(recentDesc);
     *
     *  3. In processElement():
     *       - read count.value(); if null, treat as 0
     *       - increment, then count.update(newValue)
     *       - recent.add(event.category)
     *       - read all entries from recent, keep only the last 5,
     *         and recent.update(trimmedList) so the list does not grow forever
     *       - emit "userId | clicks=N | recent=[a,b,c]" via Collector
     */
    public static class EnrichWithHistory
            extends KeyedProcessFunction<String, ClickEvent, String> {

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) {
            // TODO replace this placeholder
            out.collect(event.userId + " | (state not implemented yet)");
        }
    }
}
