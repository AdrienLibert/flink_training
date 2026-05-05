package com.training.flink.exercises;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * Lesson 13 — Batch API.
 *
 * The same `DataStream` job can run in **streaming** or **batch** mode
 * by setting {@code env.setRuntimeMode(...)}. The runtime makes very
 * different choices in each mode:
 *
 * - In STREAMING, every event flows through every operator and emits
 *   incrementally; checkpoints are how state survives failure.
 * - In BATCH, the runtime knows the input is bounded; it sorts inputs
 *   to keyed operators (no per-key state needed during the agg), runs
 *   stages sequentially, and uses blocking shuffles instead of pipelined
 *   streams. No checkpoints — failure recovery re-runs the failed stage.
 *
 * Stage 1: implement {@link #pipeline} (the SAME pipeline, both modes).
 * Stage 2: run it twice via main with different runtime modes; observe
 *          that batch mode emits each per-key sum exactly once at the
 *          end, while streaming mode emits an update per input row.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.BatchVsStream"
 */
public class BatchVsStream {

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

    // ----- Stage 1 -----
    static void pipeline(StreamExecutionEnvironment env, String label) {
        // TODO: build a pipeline that:
        //   1. emits 6 elements: ("a", 1), ("b", 1), ("a", 1), ("b", 1), ("a", 1), ("c", 1)
        //   2. keyBy(t -> t.f0)
        //   3. sum(1)  // emits running totals
        //   4. addSink(new PrintSinkFunction<>(label, false));
        //
        // In STREAMING mode you'll see a/1, b/1, a/2, b/2, a/3, c/1 — running updates.
        // In BATCH mode you'll see a/3, b/2, c/1 — final values only, emitted once.
        throw new UnsupportedOperationException("Implement pipeline");
    }
}
