package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 4 — Solution: Custom Partitioner
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise4_CustomPartitioner_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   What's the risk of a custom partitioner that doesn't take the total
 *   number of partitions into account?
 *
 * ANSWER:
 *   If your partitioner returns a fixed bucket id without modulo'ing by
 *   numPartitions:
 *
 *     - At parallelism = 4, returning 5 means "subtask 5" — but only 0..3
 *       exist. Flink throws ArrayIndexOutOfBoundsException at runtime.
 *     - Even if all bucket ids fit in current parallelism, the SAME job
 *       will fail when redeployed with lower parallelism (very common in
 *       prod: scale up for peak, scale down off-peak).
 *
 *   The fix is the one-liner this solution uses:
 *       return bucket % numPartitions;
 *
 *   The numPartitions argument is provided BY FLINK at runtime — it
 *   reflects the operator's CURRENT parallelism, which can change between
 *   runs (savepoint/restore at a different parallelism is a normal Flink
 *   operation).
 *
 *   Bonus subtle issue:
 *     - If the bucket distribution is uneven (e.g. 90% of records fall
 *       into bucket 0), modulo doesn't fix that — you'll still have skew.
 *       Custom partitioners can EXPRESS skew but cannot FIX it.
 * --------------------------------------------------------------------------
 */
public class Exercise4_CustomPartitioner_Solution {

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

        events
                .partitionCustom(new PriceBucketPartitioner(), e -> e.price)
                .map(new SubtaskTagger<>())
                .print("BUCKETED");

        env.execute("Exercise 4 - Custom Partitioner (solution)");
    }

    public static class PriceBucketPartitioner implements Partitioner<Double> {
        @Override
        public int partition(Double price, int numPartitions) {
            int bucket;
            if (price < 50.0)        bucket = 0;
            else if (price < 200.0)  bucket = 1;
            else if (price < 1000.0) bucket = 2;
            else                     bucket = 3;
            return bucket % numPartitions;   // ALWAYS modulus — see bonus answer
        }
    }
}
