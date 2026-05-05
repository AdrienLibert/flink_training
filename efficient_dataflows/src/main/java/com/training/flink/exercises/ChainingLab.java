package com.training.flink.exercises;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

/**
 * Lesson 10 — Efficient Dataflows.
 *
 * The job graph (logical) and the JobGraph (physical) are different: the
 * physical one chains adjacent operators when their parallelism matches and
 * the connection is a forward exchange. Chained operators run in the same
 * task thread, no serialization. Breaking chains has a real cost — and
 * sometimes a real benefit (isolating an expensive operator, separate
 * resource specs).
 *
 * Stage 1: print the task count for a fully-chained pipeline.
 * Stage 2: insert .startNewChain() and observe the count rise.
 * Stage 3: insert .disableChaining() on a single op and observe again.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.ChainingLab"
 */
public class ChainingLab {

    public static void main(String[] args) throws Exception {
        report("V0_default_chained", buildV0());
        report("V1_start_new_chain", buildV1());
        report("V2_disable_one_op", buildV2());
    }

    static void report(String label, StreamExecutionEnvironment env) {
        StreamGraph sg = env.getStreamGraph();
        int verticesInJobGraph = StreamingJobGraphGenerator.createJobGraph(sg)
                .getNumberOfVertices();
        int verticesInStreamGraph = sg.getStreamNodes().size();
        System.out.printf("%-22s | streamNodes=%2d | jobGraphTasks=%2d%n",
                label, verticesInStreamGraph, verticesInJobGraph);
    }

    // ----- Stage 1 -----
    static StreamExecutionEnvironment buildV0() {
        // TODO: build a stream with default chaining:
        //   env.fromElements("a", "b", "c")
        //      .map(...) .filter(...) .map(...) .print();
        // Don't pass execute() — we only inspect the graph.
        // Hint: StreamExecutionEnvironment.createLocalEnvironment(1).
        throw new UnsupportedOperationException("Implement V0");
    }

    // ----- Stage 2 -----
    static StreamExecutionEnvironment buildV1() {
        // TODO: same shape as V0 but call .startNewChain() between
        //       the first map and the filter.
        throw new UnsupportedOperationException("Implement V1");
    }

    // ----- Stage 3 -----
    static StreamExecutionEnvironment buildV2() {
        // TODO: same shape as V0 but call .disableChaining() on the filter,
        //       which forces it into its own task AND prevents it from
        //       chaining backward or forward.
        throw new UnsupportedOperationException("Implement V2");
    }
}
