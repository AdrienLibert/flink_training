package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 1: Forward vs Rebalance vs Rescale
 *
 * Goal:
 *   Observe the THREE most common Flink partitioning strategies:
 *
 *   1. forward       — the default, used when upstream and downstream
 *                       parallelism match. Records stay in the same subtask.
 *   2. rebalance()   — round-robin across ALL downstream subtasks. Cures
 *                       skew but means a network shuffle.
 *   3. rescale()     — round-robin within a LOCAL group only (cheaper
 *                       than rebalance, but only works when downstream
 *                       parallelism is a multiple of upstream).
 *
 *   You'll generate 16 events from a parallelism-1 source, then send them
 *   through a parallelism-4 mapper using each strategy in turn, and PRINT
 *   the subtask that received each record.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise1_ForwardVsRebalance"
 *
 * What to observe:
 *   - "FORWARD"  : all 16 records land on a SINGLE subtask. Why?
 *   - "REBALANCE": records spread roughly 4-4-4-4 across subtasks.
 *   - "RESCALE"  : depends on the upstream/downstream ratio. Try changing
 *                  source parallelism to 2 and see what changes.
 *
 * Bonus question (answer in a comment):
 *   When would you intentionally KEEP forward partitioning despite skew?
 *   (Hint: think about ordering guarantees, network cost, and chaining.)
 */
public class Exercise1_ForwardVsRebalance {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Source at parallelism 1 — all events come from a single subtask.
        DataStream<ClickEvent> source = env.fromElements(makeEvents()).setParallelism(1);

        // TODO 1: FORWARD — the default. Just call .map(new SubtaskTagger<>()) on
        //   the source with no partitioning, and print with a clear label.
        //   Expected: every record reports the SAME subtask index.
        //
        // source
        //     .map(new SubtaskTagger<>())
        //     .print("FORWARD");

        // TODO 2: REBALANCE — explicitly round-robin across all 4 downstream subtasks.
        //   Hint: source.rebalance().map(...).print("REBALANCE");
        //   Expected: subtasks 0..3 each get ~4 records.

        // TODO 3: RESCALE — local round-robin.
        //   Hint: source.rescale().map(...).print("RESCALE");
        //   Expected: with source parallelism=1 and downstream=4, behaves
        //   almost like rebalance because there's nothing to "rescale to."
        //   Try setting setParallelism(2) on the source to see the
        //   difference.

        env.execute("Exercise 1 - Forward vs Rebalance");
    }

    /** 16 simple events — userIds u0..u15. */
    private static ClickEvent[] makeEvents() {
        ClickEvent[] events = new ClickEvent[16];
        String[] cats = {"books", "electronics", "clothing", "toys"};
        for (int i = 0; i < 16; i++) {
            events[i] = new ClickEvent("u" + i, cats[i % 4], 10.0 + i, 1000L + i);
        }
        return events;
    }
}
