package com.training.flink.util;

import com.training.flink.model.ClickEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A tiny SourceFunction that emits a fixed sequence of ClickEvents with
 * controllable inter-event delays. Used by the TTL exercise so we can
 * actually OBSERVE state expiry in real (processing) time.
 *
 * Each entry of {@code delaysMs} says: wait this many milliseconds, then
 * emit the corresponding event.
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
