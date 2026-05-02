package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.SubtaskTagger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 1 — Solution: Forward vs Rebalance vs Rescale
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise1_ForwardVsRebalance_Solution"
 *
 * What you'll see in the output:
 *   - FORWARD   : with PARALLELISM MISMATCH (source=1, map=4), Flink can't
 *                 do true forward (which requires matching parallelism), so
 *                 it falls back to round-robin. Records spread across
 *                 multiple downstream subtasks. True forward would keep
 *                 each upstream subtask's records on the matching
 *                 downstream subtask, enabling operator chaining (no
 *                 serialization, no network, same thread).
 *   - REBALANCE : records spread roughly 4-4-4-4 across subtasks 0..3.
 *                 Explicit round-robin across the FULL set of downstream
 *                 subtasks. Network shuffle, uniform load.
 *   - RESCALE   : looks like rebalance here because upstream parallelism is
 *                 only 1. With upstream=2 and downstream=4, each upstream
 *                 subtask only round-robins to its 2 "child" downstream
 *                 subtasks — half the network cost of rebalance.
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   When would you intentionally KEEP forward partitioning despite skew?
 *
 * ANSWER:
 *   Three good reasons:
 *
 *   1. ORDER PRESERVATION across operators. forward keeps records in their
 *      original subtask, so per-key (and per-source) ordering is preserved
 *      end-to-end. rebalance/rescale break this — record N+1 may overtake
 *      record N if it lands on a different subtask with a shorter queue.
 *
 *   2. OPERATOR CHAINING. Adjacent operators with the same parallelism and
 *      forward partitioning can be CHAINED into a single thread, eliminating
 *      serialization, network buffers, and a thread switch. This is one of
 *      the biggest wins Flink gets vs other frameworks. Inserting a
 *      rebalance() breaks the chain.
 *
 *   3. SOURCE-LEVEL LOCALITY. If your source already partitions by key
 *      (e.g. Kafka with keyed messages), the data is ALREADY balanced.
 *      A keyBy or rebalance just adds a network shuffle on top for no gain.
 * --------------------------------------------------------------------------
 */
public class Exercise1_ForwardVsRebalance_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<ClickEvent> source = env.fromElements(makeEvents()).setParallelism(1);

        // FORWARD: no partitioning call — Flink uses default routing
        source.map(new SubtaskTagger<>()).print("FORWARD");

        // REBALANCE: explicit round-robin across all 4 downstream subtasks
        source.rebalance().map(new SubtaskTagger<>()).print("REBALANCE");

        // RESCALE: local round-robin (effectively rebalance with upstream=1,
        // but cheaper when upstream parallelism > 1).
        source.rescale().map(new SubtaskTagger<>()).print("RESCALE");

        env.execute("Exercise 1 - Forward vs Rebalance (solution)");
    }

    private static ClickEvent[] makeEvents() {
        ClickEvent[] events = new ClickEvent[16];
        String[] cats = {"books", "electronics", "clothing", "toys"};
        for (int i = 0; i < 16; i++) {
            events[i] = new ClickEvent("u" + i, cats[i % 4], 10.0 + i, 1000L + i);
        }
        return events;
    }
}
