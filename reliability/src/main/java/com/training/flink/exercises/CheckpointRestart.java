package com.training.flink.exercises;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Lesson 18 — Reliability.
 *
 * This job demonstrates checkpoint+restart end-to-end:
 *
 *   1. A counting source emits 30 records, with a checkpointable
 *      cursor in operator state.
 *   2. A FlakyMap throws on its 12th input. The operator dies; the
 *      runtime detects the failure; the FixedDelayRestartStrategy
 *      restarts the job from the latest checkpoint.
 *   3. After restart, the source resumes from the cursor it
 *      checkpointed before the crash. The FlakyMap counter resets to
 *      its restored value (which is the last checkpointed value, NOT 0)
 *      and proceeds past 12 without crashing again.
 *
 * Stage 1: implement {@link FlakyMap#map} so it throws once.
 * Stage 2: enable checkpointing in main with EXACTLY_ONCE mode.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.CheckpointRestart"
 */
public class CheckpointRestart {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Stage 2: enable checkpointing + restart strategy.
        // TODO: env.enableCheckpointing(200L, CheckpointingMode.EXACTLY_ONCE);
        //       env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(1)));
        //
        // Then build the pipeline:
        //   env.addSource(new CountingSource(30))
        //      .returns(Types.INT)
        //      .map(new FlakyMap())
        //      .returns(Types.INT)
        //      .print("OUT");
        //
        //   env.execute("Lesson 18 — Checkpoint Restart");
        throw new UnsupportedOperationException("Implement Stage 2");
    }

    /** Counting source emitting `n` records, cursor checkpointed in operator state. */
    public static class CountingSource
            implements SourceFunction<Integer>, CheckpointedFunction {
        private final int n;
        private long cursor = 0L;
        private volatile boolean running = true;
        private transient ListState<Long> cursorState;

        public CountingSource(int n) {
            this.n = n;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running && cursor < n) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect((int) cursor);
                    cursor++;
                }
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            cursorState.update(java.util.Collections.singletonList(cursor));
            System.out.println("[source]  checkpoint @ cursor=" + cursor);
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            cursorState = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("cursor", Types.LONG));
            if (ctx.isRestored()) {
                for (Long c : cursorState.get()) {
                    cursor = c;
                }
                System.out.println("[source]  RESTORED cursor=" + cursor);
            }
        }
    }

    /** Map that throws once, on its 12th input — `haveCrashed` is static so it
     *  survives the restart (one-shot simulation). */
    public static class FlakyMap extends RichMapFunction<Integer, Integer> {
        private static volatile boolean haveCrashed = false;
        private long seen = 0L;

        @Override
        public Integer map(Integer x) {
            // TODO Stage 1:
            //   - increment `seen`.
            //   - if seen >= 12 and !haveCrashed, set haveCrashed = true and
            //     throw new RuntimeException("simulated failure at seen=" + seen).
            //   - otherwise return x as-is.
            throw new UnsupportedOperationException("Implement FlakyMap.map");
        }
    }
}
