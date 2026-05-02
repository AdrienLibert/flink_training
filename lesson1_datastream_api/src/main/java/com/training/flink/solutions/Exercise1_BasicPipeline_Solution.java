package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 1 — Solution: Basic Transformations
 *
 * filter purchases → map to (category, price) → print
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise1_BasicPipeline_Solution"
 *
 * Key takeaway:
 *   When a lambda changes the output type and that type uses generics
 *   (Tuple2, List<X>, Optional<X>, ...), Java's type erasure prevents Flink
 *   from inferring it. You MUST hint the type with .returns(...) — otherwise
 *   Flink throws InvalidTypesException at job-graph build time.
 */
public class Exercise1_BasicPipeline_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "p100", "electronics", 299.99, 1000L, "view"),
                new ClickEvent("u2", "p200", "books",        19.50, 2000L, "purchase"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 3000L, "add_to_cart"),
                new ClickEvent("u3", "p300", "clothing",     49.00, 4000L, "purchase"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 5000L, "purchase"),
                new ClickEvent("u4", "p400", "books",        12.00, 6000L, "view"),
                new ClickEvent("u2", "p500", "clothing",     89.99, 7000L, "purchase")
        );

        DataStream<Tuple2<String, Double>> categoryPrices = clicks
                .filter(e -> "purchase".equals(e.action))
                .map(e -> new Tuple2<>(e.category, e.price))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));

        categoryPrices.print();

        env.execute("Exercise 1 - Basic Pipeline (solution)");
    }
}
