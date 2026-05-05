package com.training.flink.exercises;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Lesson 21 — Performance & Tuning.
 *
 * Object reuse is the highest-leverage code-level optimization for
 * Flink jobs that are CPU-bound on serialization. By default, Flink
 * defensively copies records between operators. With
 * {@code env.getConfig().enableObjectReuse()}, it doesn't.
 *
 * The catch: if your operators MUTATE incoming records, object reuse
 * breaks correctness. Most operators don't — they read fields and
 * emit new objects. But a stateful map that holds onto a reference
 * and then sees the next record overwrite it is suddenly buggy.
 *
 * This demo runs a pipeline twice (object-reuse off, then on) and
 * prints both outputs. The output should be identical IF the pipeline
 * is reuse-safe.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.ObjectReuseDemo"
 */
public class ObjectReuseDemo {

    public static void main(String[] args) throws Exception {
        runWith(false, "OFF");
        runWith(true, "ON");
    }

    static void runWith(boolean reuse, String label) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        if (reuse) {
            env.getConfig().enableObjectReuse();
        }

        env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("a", 3),
                Tuple2.of("c", 4),
                Tuple2.of("a", 5)
        ).returns(Types.TUPLE(Types.STRING, Types.INT))
         .keyBy(t -> t.f0)
         .sum(1)                                 // safe: aggregator doesn't hold refs
         .map(t -> Tuple2.of(t.f0, t.f1 * 10))   // safe: emits new tuple
         .returns(Types.TUPLE(Types.STRING, Types.INT))
         .print(label);

        env.execute("ObjectReuse=" + label);
    }
}
