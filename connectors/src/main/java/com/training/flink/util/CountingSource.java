package com.training.flink.util;

import com.training.flink.model.Order;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A checkpointable SourceFunction emitting `count` deterministic Order rows.
 *
 * The "current cursor" is stored in a ListState so a savepoint+restore would
 * resume from where it left off. We use this in the lesson to talk about the
 * SourceFunction → Source V2 migration.
 */
public class CountingSource implements SourceFunction<Order>, CheckpointedFunction {

    private final long count;
    private long cursor = 0L;
    private volatile boolean running = true;
    private transient ListState<Long> cursorState;

    public CountingSource(long count) {
        this.count = count;
    }

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (running && cursor < count) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new Order(
                        "o" + cursor,
                        "u" + (cursor % 4),
                        10.0 + (cursor % 5) * 5.0));
                cursor++;
            }
            Thread.sleep(20);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        cursorState.update(java.util.Collections.singletonList(cursor));
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        cursorState = ctx.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("cursor", Types.LONG));
        if (ctx.isRestored()) {
            for (Long c : cursorState.get()) {
                cursor = c;
            }
        }
    }
}
