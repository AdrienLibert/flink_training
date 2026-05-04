package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 2: KeyBy + Aggregation
 *
 * Goal:
 *   Compute the TOTAL REVENUE PER CATEGORY in real time.
 *   - Filter only "purchase" events
 *   - Map to (category, price)
 *   - Key by category
 *   - Reduce / sum to accumulate revenue per category
 *   - Print the running totals
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise2_RevenuePerCategory"
 *
 * Expected behaviour: each new purchase emits an UPDATED total for that category.
 * (Not a single final number. Streaming = continuous output.)
 *
 * Question to think about (write your answer in the QUESTION_ANSWER comment below):
 *   Why does the output show MULTIPLE totals for the same category instead of a single final number?
 */
public class Exercise2_RevenuePerCategory {

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
                .filter(e -> "purchase".equals(e.getAction()))
                .map(e -> new Tuple2<>(e.getCategory(), e.getPrice()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));

        KeyedStream<Tuple2<String, Double>, String> keyed = categoryPrices.keyBy(t -> t.f0);
        DataStream<Tuple2<String, Double>> revenue = keyed.reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
        revenue.print();

        env.execute("Exercise 2 - Revenue Per Category");
    }
}
