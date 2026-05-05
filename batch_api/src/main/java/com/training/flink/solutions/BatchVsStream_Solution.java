package com.training.flink.solutions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class BatchVsStream_Solution {

    public static void main(String[] args) throws Exception {
        run(RuntimeExecutionMode.STREAMING, "STREAM");
        run(RuntimeExecutionMode.BATCH, "BATCH");
    }

    static void run(RuntimeExecutionMode mode, String label) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(mode);
        env.setParallelism(1);

        pipeline(env, label);

        env.execute("Lesson 13 — " + label);
    }

    static void pipeline(StreamExecutionEnvironment env, String label) {
        DataStream<Tuple2<String, Integer>> events = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("b", 1),
                Tuple2.of("a", 1),
                Tuple2.of("b", 1),
                Tuple2.of("a", 1),
                Tuple2.of("c", 1)
        ).returns(Types.TUPLE(Types.STRING, Types.INT));

        events
                .keyBy(t -> t.f0)
                .sum(1)
                .addSink(new PrintSinkFunction<>(label, false));
    }
}
