package com.training.flink.solutions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

public class ChainingLab_Solution {

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

    static StreamExecutionEnvironment buildV0() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<String> s = env.fromElements("a", "b", "c");
        s.map(x -> x.toUpperCase())
                .filter(x -> !x.equals("B"))
                .map(x -> "<" + x + ">")
                .print();
        return env;
    }

    static StreamExecutionEnvironment buildV1() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<String> s = env.fromElements("a", "b", "c");
        s.map(x -> x.toUpperCase())
                .startNewChain()
                .filter(x -> !x.equals("B"))
                .map(x -> "<" + x + ">")
                .print();
        return env;
    }

    static StreamExecutionEnvironment buildV2() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<String> s = env.fromElements("a", "b", "c");
        s.map(x -> x.toUpperCase())
                .filter(x -> !x.equals("B")).disableChaining()
                .map(x -> "<" + x + ">")
                .print();
        return env;
    }
}
