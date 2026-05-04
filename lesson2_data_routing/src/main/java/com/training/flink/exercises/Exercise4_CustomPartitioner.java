package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 4: Custom Partitioner
 *
 * Goal:
 *   Sometimes neither hash partitioning (keyBy) nor round-robin (rebalance)
 *   is what you want. A custom Partitioner<T> lets you write the routing
 *   logic by hand.
 *
 *   In this exercise: route ClickEvents to subtasks based on PRICE BUCKET.
 *   - price <  50      → subtask 0  (cheap)
 *   - price >= 50  & < 200  → subtask 1  (mid)
 *   - price >= 200 & < 1000 → subtask 2  (premium)
 *   - price >= 1000    → subtask 3  (luxury)
 *
 *   This is useful for cases like:
 *   - Routing high-value transactions to a dedicated processor (e.g. for
 *     extra fraud checks).
 *   - Sending events to subtasks that own specific resources (e.g. one
 *     subtask owns the connection to a downstream service for one tier).
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise4_CustomPartitioner"
 *
 * Bonus question:
 *   What's the risk of a custom partitioner that doesn't take the total
 *   number of partitions into account? (Hint: parallelism may change in
 *   prod. Always do the modulus.)
 */
public class Exercise4_CustomPartitioner {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<ClickEvent> events = env.fromElements(
                new ClickEvent("u1", "books",       10.0, 1000L),     // bucket 0
                new ClickEvent("u2", "electronics", 75.0, 2000L),     // bucket 1
                new ClickEvent("u3", "electronics", 350.0, 3000L),    // bucket 2
                new ClickEvent("u4", "watches",     5_000.0, 4000L),  // bucket 3
                new ClickEvent("u5", "clothing",    25.0, 5000L),     // bucket 0
                new ClickEvent("u6", "electronics", 150.0, 6000L),    // bucket 1
                new ClickEvent("u7", "watches",     2_500.0, 7000L),  // bucket 3
                new ClickEvent("u8", "electronics", 800.0, 8000L)     // bucket 2
        ).setParallelism(1);

        // TODO 1: implement a Partitioner<Double> below (PriceBucketPartitioner)
        //   that maps a price to a partition index 0..N-1.

        // TODO 2: apply it with partitionCustom.
        //   partitionCustom takes (Partitioner, KeySelector) — the KeySelector
        //   extracts the value passed to the Partitioner's partition() method.
        //
        //   events
        //       .partitionCustom(new PriceBucketPartitioner(), e -> e.price)
        //       .map(new SubtaskTagger<>())
        //       .print("BUCKETED");
        //
        //   Expected: bucket 0 records → subtask 0, bucket 1 → subtask 1, etc.

        env.execute("Exercise 4 - Custom Partitioner");
    }

    // TODO 3: implement PriceBucketPartitioner.
    //
    //   public static class PriceBucketPartitioner implements Partitioner<Double> {
    //       @Override
    //       public int partition(Double price, int numPartitions) {
    //           int bucket;
    //           if (price < 50)        bucket = 0;
    //           else if (price < 200)  bucket = 1;
    //           else if (price < 1000) bucket = 2;
    //           else                   bucket = 3;
    //           return bucket % numPartitions;   // <-- ALWAYS modulus!
    //       }
    //   }
}
