package com.training.flink.solutions;

import com.training.flink.model.Alert;
import com.training.flink.model.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Exercise 3 — Solution: Abandoned Cart Detection
 *
 * KeyedProcessFunction with ValueState<Long> + EVENT-TIME timer.
 * Reset semantics: a new add_to_cart cancels the previous timer and starts a
 * fresh 5s window.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise3_AbandonedCart_Solution"
 *
 * Expected output (order may vary across subtasks):
 *   Alert{userId='u2', reason='abandoned cart'}
 *   Alert{userId='u3', reason='abandoned cart'}
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION 1:
 *   Switch to event-time timers. What watermark strategy do you need?
 *
 * ANSWER:
 *   Event-time timers fire when the WATERMARK passes their timestamp. Without
 *   a watermark strategy, the operator never advances event time and timers
 *   never fire.
 *
 *   In our test data the timestamps are strictly increasing, so the simplest
 *   correct strategy is `WatermarkStrategy.forMonotonousTimestamps()`. Apply
 *   it via `assignTimestampsAndWatermarks(...)` BEFORE the keyBy, with a
 *   timestamp assigner that returns `event.timestamp`.
 *
 *   In real systems, events almost always arrive out of order (network jitter,
 *   multiple shards, retries). For that, use:
 *     WatermarkStrategy
 *       .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(N))
 *       .withTimestampAssigner((e, ts) -> e.timestamp)
 *
 *   Choose N (the out-of-orderness bound) based on observed lateness in
 *   prod — too small means you drop late events; too large means timers
 *   fire late and end-of-day reports lag.
 *
 *   Why event-time matters here specifically: with a BOUNDED source like
 *   fromElements(), processing-time timers NEVER fire — when the source
 *   ends, pending processing-time timers are silently discarded. Event-time
 *   timers, by contrast, get one final flush via a Long.MAX_VALUE watermark
 *   before shutdown.
 * --------------------------------------------------------------------------
 * BONUS QUESTION 2:
 *   What happens if the user adds_to_cart twice without purchasing?
 *   Should the timer be reset, or should the original timer still fire?
 *
 * ANSWER:
 *   Reset (the choice this solution implements). Justification:
 *
 *   - Real-world abandonment is about INACTIVITY. A user who keeps adding
 *     items is engaged; alerting them defeats the purpose.
 *   - "Original-timer-still-fires" would penalize active shoppers and
 *     produce false positives.
 *
 *   Implementation choice: delete the old timer and register a new one when
 *   a second add_to_cart arrives (this file's approach). Equivalent
 *   alternative: keep all timers alive and, in onTimer, only emit if the
 *   fired timestamp matches the one currently in state — stale timers fire
 *   harmlessly. The Flink docs slightly prefer the second approach because
 *   deleting timers has cost; both are correct.
 * --------------------------------------------------------------------------
 */
public class Exercise3_AbandonedCart_Solution {

    private static final long TIMEOUT_MS = 5_000L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "p100", "electronics", 299.99, 1000L, "add_to_cart"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 1500L, "purchase"),
                new ClickEvent("u2", "p200", "books",        19.50, 2000L, "add_to_cart"),
                new ClickEvent("u3", "p300", "clothing",     49.00, 3000L, "add_to_cart"),
                new ClickEvent("u3", "p301", "clothing",     89.00, 3500L, "view"),
                new ClickEvent("u4", "p400", "books",        12.00, 4000L, "purchase"),
                new ClickEvent("u5", "p500", "clothing",     39.99, 5000L, "add_to_cart"),
                new ClickEvent("u5", "p501", "clothing",     59.99, 5500L, "add_to_cart"),
                new ClickEvent("u5", "p501", "clothing",     59.99, 6000L, "purchase")
        );

        DataStream<ClickEvent> withTimestamps = clicks.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, recordTimestamp) -> event.timestamp)
        );

        KeyedStream<ClickEvent, String> keyed = withTimestamps.keyBy(e -> e.userId);

        DataStream<Alert> alerts = keyed.process(new AbandonedCartDetector());

        alerts.print();

        env.execute("Exercise 3 - Abandoned Cart (solution)");
    }

    public static class AbandonedCartDetector
            extends KeyedProcessFunction<String, ClickEvent, Alert> {

        private transient ValueState<Long> pendingTimer;

        @Override
        public void open(Configuration parameters) {
            pendingTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pendingTimer", Long.class));
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<Alert> out)
                throws Exception {
            String action = event.action;

            if ("add_to_cart".equals(action)) {
                // RESET: cancel the previous timer (if any) so this user's window restarts
                Long previous = pendingTimer.value();
                if (previous != null) {
                    ctx.timerService().deleteEventTimeTimer(previous);
                }
                long fireAt = ctx.timestamp() + TIMEOUT_MS;
                ctx.timerService().registerEventTimeTimer(fireAt);
                pendingTimer.update(fireAt);

            } else if ("purchase".equals(action)) {
                // Cancel any pending alert and clear state
                Long previous = pendingTimer.value();
                if (previous != null) {
                    ctx.timerService().deleteEventTimeTimer(previous);
                    pendingTimer.clear();
                }
            }
            // All other actions ("view", etc.) are intentionally ignored.
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out)
                throws Exception {
            out.collect(new Alert(ctx.getCurrentKey(), "abandoned cart"));
            pendingTimer.clear();
        }
    }
}
