package com.training.flink.solutions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Collections;

public class CheckpointRestart_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(1)));

        env.addSource(new CountingSource(30))
                .returns(Types.INT)
                .map(new FlakyMap())
                .returns(Types.INT)
                .print("OUT");

        env.execute("Lesson 18 — Checkpoint Restart");
    }

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

    public static class FlakyMap extends RichMapFunction<Integer, Integer> {
        // Static so the "have we crashed yet?" flag survives restart within
        // the same JVM. In production the crash trigger is something
        // genuinely external (bad data, OOM); here we just want a one-shot
        // simulation.
        private static volatile boolean haveCrashed = false;
        private long seen = 0L;

        @Override
        public Integer map(Integer x) {
            seen++;
            if (seen >= 12 && !haveCrashed) {
                haveCrashed = true;
                throw new RuntimeException("simulated failure at seen=" + seen);
            }
            return x;
        }
    }
}
