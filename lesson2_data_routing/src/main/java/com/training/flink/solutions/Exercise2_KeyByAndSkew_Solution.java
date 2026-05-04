package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 2 — Solution: KeyBy under Skew
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise2_KeyByAndSkew_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION 1:
 *   Why is co-location of records by key a FEATURE for state correctness
 *   and a BUG for load balancing?
 *
 * ANSWER:
 *   FEATURE (state correctness):
 *     - Keyed state in Flink is "owned" by the subtask that handles the key.
 *     - All records for the same key are processed by the SAME subtask, so
 *       state reads/writes are always local — no cross-subtask coordination.
 *     - This is what makes things like "running total per user" or
 *       "abandoned cart per user" work correctly under parallelism.
 *
 *   BUG (load balancing):
 *     - If one key has 90% of the volume, ONE subtask gets 90% of the work.
 *     - The other subtasks sit idle while that one becomes a bottleneck.
 *     - Throughput is bounded by the slowest subtask. CPU and network for
 *       the rest of the cluster is wasted.
 *
 *   Skew is the streaming-systems version of "data goes where the key
 *   lives" — same trade-off as a sharded database with a hot shard.
 * --------------------------------------------------------------------------
 * BONUS QUESTION 2:
 *   Three mitigation strategies for hot-key skew?
 *
 * ANSWER:
 *   1. PRE-AGGREGATION (combiner / local pre-reduce):
 *      Aggregate at the source/upstream level BEFORE the network shuffle.
 *      Use `reduce` or `aggregate` chained directly to the source so each
 *      upstream subtask compresses its slice of the hot key into one
 *      record before sending to the keyed subtask.
 *      → Flink Table/SQL does this automatically as "mini-batch + local-global agg".
 *
 *   2. SALTED KEYS (key splitting):
 *      Replace the hot key K with N pseudo-keys K_0, K_1, ..., K_{N-1}.
 *      Distribute records to one at random. Aggregate per pseudo-key,
 *      then DOWNSTREAM combine the N partial results back into one.
 *      → Spreads load across N subtasks at the cost of an extra reduce step.
 *
 *   3. TWO-PHASE KEYING:
 *      Phase 1: keyBy(salted_key) → partial aggregation.
 *      Phase 2: keyBy(real_key)   → final aggregation across the partials.
 *      → Generalizes salting; standard pattern for skewed group-by in Flink.
 *
 *   Other valid answers: side-output the hot key to a dedicated processor
 *   (route hot keys with a custom Partitioner); use SQL hints (MINI_BATCH);
 *   use a different key entirely (e.g. user_id_hash_bucket).
 * --------------------------------------------------------------------------
 */
public class Exercise2_KeyByAndSkew_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<ClickEvent> events = env.fromElements(makeSkewedEvents()).setParallelism(1);

        // Hash partition by userId — co-locates records per key on a subtask.
        events
                .keyBy(e -> e.userId)
                .map(new SubtaskTagger<>())
                .print("KEYBY");

        // Compare: rebalance gives uniform load but breaks key co-location.
        events
                .rebalance()
                .map(new SubtaskTagger<>())
                .print("REBALANCE");

        env.execute("Exercise 2 - KeyBy and Skew (solution)");
    }

    private static ClickEvent[] makeSkewedEvents() {
        ClickEvent[] events = new ClickEvent[100];
        for (int i = 0; i < 90; i++) {
            events[i] = new ClickEvent("vip_user", "electronics", 100.0, 1000L + i);
        }
        for (int i = 0; i < 10; i++) {
            events[90 + i] = new ClickEvent("u" + i, "books", 20.0, 2000L + i);
        }
        return events;
    }
}
