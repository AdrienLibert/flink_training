package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Exercise 4 — Solution: Operator State
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise4_OperatorState_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   When to use union list state, and the danger for "buffered records"?
 *
 * ANSWER:
 *   USE UNION LIST STATE when every subtask needs the COMPLETE list on
 *   restore. Two classic cases:
 *     1. Kafka source operators that need to know every (partition, offset)
 *        in the world before they can decide which partitions THIS new
 *        subtask should own after a rescale.
 *     2. Small global config tables that change rarely and must be present
 *        on every subtask.
 *
 *   THE DANGER for "buffered records": every subtask would receive ALL
 *   buffered records on restore, so a 7-subtask job with 1k buffered events
 *   each would replay 7k events instead of 7k. Records get DUPLICATED.
 *   Worse, with union list state Flink has to gather the entire global list
 *   at every checkpoint, which doesn't scale — unbounded buffers will OOM
 *   the JobManager.
 *
 *   Rule: union list state is for SMALL, GLOBAL data that all subtasks need
 *   to see. Even-split list state is for PER-SUBTASK data like our buffer.
 * --------------------------------------------------------------------------
 */
public class Exercise4_OperatorState_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(2_000);

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "books", 10.0, 1L),
                new ClickEvent("u2", "toys", 8.0, 2L),
                new ClickEvent("u3", "books", 12.0, 3L),
                new ClickEvent("u4", "electronics", 99.0, 4L),
                new ClickEvent("u5", "clothing", 30.0, 5L),
                new ClickEvent("u6", "books", 11.0, 6L),
                new ClickEvent("u7", "toys", 9.0, 7L)
        );

        clicks
                .map(new BatchingMapper(3))
                .print();

        env.execute("Exercise 4 - Operator State (solution)");
    }

    public static class BatchingMapper
            implements MapFunction<ClickEvent, String>, CheckpointedFunction {

        private final int batchSize;
        private final List<ClickEvent> localBuffer = new ArrayList<>();
        private transient ListState<ClickEvent> buffered;

        public BatchingMapper(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public String map(ClickEvent value) {
            localBuffer.add(value);
            if (localBuffer.size() >= batchSize) {
                String flush = "FLUSH " + localBuffer.size() + " events: "
                        + localBuffer.stream()
                                .map(c -> c.category)
                                .collect(Collectors.joining(","));
                localBuffer.clear();
                return flush;
            }
            return "buffering: " + value.category + " (size=" + localBuffer.size() + ")";
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // Persist the in-memory buffer so we don't lose it on restart.
            buffered.update(new ArrayList<>(localBuffer));
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            ListStateDescriptor<ClickEvent> desc = new ListStateDescriptor<>(
                    "opBuffer",
                    TypeInformation.of(new TypeHint<ClickEvent>() {}));

            // Even-split list state: on rescale, entries are redistributed
            // evenly across the new subtasks.
            buffered = ctx.getOperatorStateStore().getListState(desc);

            if (ctx.isRestored()) {
                for (ClickEvent e : buffered.get()) {
                    localBuffer.add(e);
                }
            }
        }
    }
}
