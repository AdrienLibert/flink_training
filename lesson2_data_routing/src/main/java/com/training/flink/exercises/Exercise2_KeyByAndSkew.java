package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 2: KeyBy under Skewed Keys
 *
 * Goal:
 *   See first-hand how `keyBy` distributes records by HASHING the key, and
 *   what happens when one key dominates the data ("hot key" / data skew).
 *
 *   You'll generate 100 events where 90 share the userId "vip_user" and
 *   the other 10 are spread across u0..u9. After keyBy, observe how many
 *   records each downstream subtask receives.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise2_KeyByAndSkew"
 *
 * Expected observations:
 *   - One subtask gets ~90 records (the hot key always hashes to the same
 *     subtask — keyBy guarantees co-location of records sharing a key).
 *   - The other subtasks get a few records each.
 *   - This is the "data skew" production problem in miniature.
 *
 * Bonus questions (answer in a comment):
 *   1. Why is co-location of records by key a FEATURE for state correctness
 *      and a BUG for load balancing?
 *   2. What are 3 mitigation strategies for hot-key skew? (Hint:
 *      pre-aggregation, salted keys, two-phase keying.)
 */
public class Exercise2_KeyByAndSkew {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<ClickEvent> events = env.fromElements(makeSkewedEvents()).setParallelism(1);

        // TODO 1: keyBy(userId) → map(new SubtaskTagger<>()) → print("KEYBY")
        //
        //   events
        //       .keyBy(e -> e.userId)
        //       .map(new SubtaskTagger<>())
        //       .print("KEYBY");
        //
        //   Look at the output: count how many records each subtask got.
        //   You should see one subtask dominating with ~90 records.

        // TODO 2 (optional but recommended):
        //   ALSO try .rebalance() on the same data and print as "REBALANCE".
        //   Compare: rebalance is uniform, keyBy is skewed.
        //   This makes the cost of keyBy visible — you give up uniformity
        //   in exchange for state co-location.

        env.execute("Exercise 2 - KeyBy and Skew");
    }

    /** 90 events for "vip_user", 10 events for u0..u9. */
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
