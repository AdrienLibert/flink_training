package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Exercise 2: MapState + ReducingState
 *
 * Goal:
 *   For each user, on every click, output:
 *     "u1 | spend=64.00 | byCategory={books=2, toys=1, electronics=1}"
 *
 *   Use:
 *     - MapState&lt;String,Long&gt;        category → count
 *     - ReducingState&lt;Double&gt;        running total spend (auto-summed)
 *
 * Concepts:
 *   - MapState looks like a HashMap but each entry is its own row in the
 *     state backend. Iterating is fine; you don't deserialize the whole
 *     map for a single put.
 *   - ReducingState applies a ReduceFunction on every .add(x). The state
 *     stores ONE accumulated value, not the history. Perfect for sums,
 *     mins, maxes, etc.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise2_MapAndReducingState"
 *
 * Bonus question:
 *   You could implement the per-category counter with ValueState&lt;HashMap&gt;
 *   instead of MapState. List two reasons MapState is better, and one
 *   reason you might still pick ValueState&lt;HashMap&gt; despite that.
 */
public class Exercise2_MapAndReducingState {

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

        env.execute("Exercise 2 - MapState + ReducingState");
    }

    /**
     * TODO:
     *   1. Declare:
     *        MapStateDescriptor<String,Long>      catCountsDesc
     *        ReducingStateDescriptor<Double>      spendDesc
     *           — pass a ReduceFunction&lt;Double&gt; that returns a + b
     *
     *   2. open(): get the state handles via:
     *        getRuntimeContext().getMapState(catCountsDesc)
     *        getRuntimeContext().getReducingState(spendDesc)
     *
     *   3. processElement():
     *        - increment counts by reading catCounts.get(category) (null → 0)
     *          and writing back catCounts.put(category, n+1)
     *        - spend.add(event.price)   // ReducingState merges automatically
     *        - emit "userId | spend=… | byCategory={…}" using the iterator
     *          from catCounts.entries()
     */
    public static class SummarizePerUser
            extends KeyedProcessFunction<String, ClickEvent, String> {

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) {
            // TODO replace this placeholder
            out.collect(event.userId + " | (state not implemented yet)");
        }
    }
}
