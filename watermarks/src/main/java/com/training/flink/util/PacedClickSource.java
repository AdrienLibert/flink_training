package com.training.flink.util;

import com.training.flink.model.ClickEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A tiny SourceFunction that emits a fixed sequence of ClickEvents with
 * controllable inter-event delays. This makes watermark behaviour visible:
 * with {@code env.fromElements(...)}, all records flush in milliseconds and
 * {@code forBoundedOutOfOrderness}'s 200ms periodic watermark generator
 * never gets a chance to advance the watermark between events — so late
 * events look "on-time" because windows haven't fired yet.
 *
 * Pace events ~300ms apart and the watermark progresses between each one,
 * making late vs. very-late dispositions actually observable.
 */
public class PacedClickSource implements SourceFunction<ClickEvent> {

    private final ClickEvent[] events;
    private final long[] delaysMs;
    private volatile boolean running = true;

    public PacedClickSource(ClickEvent[] events, long[] delaysMs) {
        if (events.length != delaysMs.length) {
            throw new IllegalArgumentException("events and delaysMs must be same length");
        }
        this.events = events;
        this.delaysMs = delaysMs;
    }

    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        for (int i = 0; i < events.length && running; i++) {
            Thread.sleep(delaysMs[i]);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(events[i]);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
