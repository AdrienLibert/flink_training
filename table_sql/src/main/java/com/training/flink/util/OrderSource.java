package com.training.flink.util;

import com.training.flink.model.Order;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Bounded, finite source: emits 12 deterministic orders covering 3 categories
 * across a ~15-second event-time span. Sleeps 50ms between emissions so the
 * TableEnvironment has time to build and schedule the job graph before the
 * source completes.
 */
public class OrderSource implements SourceFunction<Order> {

    private static final Order[] ORDERS = new Order[] {
            new Order("o1",  "u1", "books",       12.0,  1_000L),
            new Order("o2",  "u2", "electronics", 80.0,  1_500L),
            new Order("o3",  "u1", "books",        8.0,  2_000L),
            new Order("o4",  "u3", "electronics",100.0,  2_500L),
            new Order("o5",  "u2", "clothing",    25.0,  3_000L),
            new Order("o6",  "u1", "books",       15.0,  4_500L),
            new Order("o7",  "u3", "electronics", 60.0,  6_000L),
            new Order("o8",  "u2", "clothing",    40.0,  7_500L),
            new Order("o9",  "u1", "books",       20.0,  8_000L),
            new Order("o10", "u3", "clothing",    30.0, 11_000L),
            new Order("o11", "u2", "electronics", 90.0, 12_000L),
            new Order("o12", "u1", "books",       11.0, 13_500L),
    };

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        for (Order o : ORDERS) {
            if (!running) return;
            synchronized (ctx.getCheckpointLock()) {
                ctx.collectWithTimestamp(o, o.ts);
            }
            Thread.sleep(50);
        }
        // Emit a final watermark so any event-time windows fire before stream end.
        synchronized (ctx.getCheckpointLock()) {
            ctx.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(Long.MAX_VALUE));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
