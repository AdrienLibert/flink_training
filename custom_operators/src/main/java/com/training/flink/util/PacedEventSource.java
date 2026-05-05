package com.training.flink.util;

import com.training.flink.model.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** Emits 12 events at ~50ms cadence so processing-time timers can actually fire. */
public class PacedEventSource implements SourceFunction<Event> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        long v = 0;
        String[] keys = {"a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b", "c"};
        for (int i = 0; i < keys.length && running; i++) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new Event(keys[i], v++));
            }
            Thread.sleep(50);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
