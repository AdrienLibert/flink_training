package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 1: Basic Transformations
 *
 * Goal:
 *   1. Read from a source of ClickEvent (provided below).
 *   2. Filter only "purchase" actions.
 *   3. Map each purchase to a Tuple2<String, Double> of (category, price).
 *   4. Print the result.
 *
 * Run from this directory:
 *   mvn compile exec:java -Dexec.mainClass="com.training.flink.exercises.Exercise1_BasicPipeline"
 *
 * Or run main() directly from your IDE.
 */
public class Exercise1_BasicPipeline {

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

        DataStream<ClickEvent> purchases = clicks.filter(events -> "purchase".equals(events.getAction()));
        DataStream<Tuple2<String, Double>> categoryPrices = purchases
                .map(events -> new Tuple2<>(events.getCategory(), events.getPrice()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
        
        categoryPrices.print();
        
        env.execute("Exercise 1 - Basic Pipeline");
    }
}
