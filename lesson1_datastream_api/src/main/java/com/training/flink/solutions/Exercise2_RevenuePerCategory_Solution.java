package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 2 — Solution: Revenue Per Category
 *
 * keyBy(category) → reduce(sum prices) → print running totals.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise2_RevenuePerCategory_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   Why does the output show MULTIPLE totals for the same category instead
 *   of a single final number?
 *
 * ANSWER:
 *   Streams are conceptually unbounded — there is no "end of input" at which
 *   to compute a single final answer. Every record that arrives is a NEW fact,
 *   and `reduce` is incremental: for each new event it updates the keyed
 *   state and emits the new running total. So you see one output PER INPUT
 *   event for that key, not one per key total.
 *
 *   To get "one final number per category", you must add a notion of
 *   completeness — typically a window (tumbling/sliding/session) that
 *   defines a slice of the stream and emits a result when the slice closes.
 *
 *   Bonus insight: in Flink's changelog semantics (Table API), this same
 *   running-total stream is a "retract stream" — each row updates the
 *   previous one. The DataStream API doesn't carry retraction info; the
 *   sink sees an append-only stream of (category, runningTotal) snapshots.
 * --------------------------------------------------------------------------
 */
public class Exercise2_RevenuePerCategory_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "p100", "electronics", 299.99, 1000L, "view"),
                new ClickEvent("u2", "p200", "books",        19.50, 2000L, "purchase"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 3000L, "add_to_cart"),
                new ClickEvent("u3", "p300", "clothing",     49.00, 4000L, "purchase"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 5000L, "purchase"),
                new ClickEvent("u4", "p400", "books",        12.00, 6000L, "view"),
                new ClickEvent("u2", "p500", "clothing",     89.99, 7000L, "purchase"),
                new ClickEvent("u5", "p600", "books",        25.00, 8000L, "purchase"),
                new ClickEvent("u6", "p700", "clothing",    150.00, 9000L, "purchase")
        );

        DataStream<Tuple2<String, Double>> categoryPrices = clicks
                .filter(e -> "purchase".equals(e.action))
                .map(e -> new Tuple2<>(e.category, e.price))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));

        KeyedStream<Tuple2<String, Double>, String> keyed = categoryPrices.keyBy(t -> t.f0);

        // a = previous accumulated result (from state), b = new incoming element.
        // Sum the prices in f1; keep the category in f0.
        DataStream<Tuple2<String, Double>> revenue = keyed.reduce(
                (a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1)
        );

        revenue.print();

        env.execute("Exercise 2 - Revenue Per Category (solution)");
    }
}
