package com.training.flink.exercises;

import com.training.flink.model.Alert;
import com.training.flink.model.ClickEvent;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;


/**
 * Exercise 3: Abandoned Cart Detection
 *
 * Goal:
 *   Detect users who added an item to their cart but never purchased it
 *   within a configurable timeout window.
 *
 * Requirements:
 *   - keyBy userId
 *   - Use a KeyedProcessFunction<String, ClickEvent, Alert>
 *   - Use ValueState<Long> to remember when the user last did "add_to_cart"
 *   - Register a processing-time timer for "now + TIMEOUT_MS"
 *   - If a "purchase" arrives before the timer fires:
 *         * clear the state
 *         * cancel/ignore the timer
 *   - If the timer fires and state is still set:
 *         * emit Alert(userId, "abandoned cart")
 *         * clear the state
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise3_AbandonedCart"
 *
 * Bonus:
 *   - Switch to event-time timers. What watermark strategy do you need?
 *   - What happens if the user adds_to_cart twice without purchasing?
 *     Should the timer be reset, or should the original timer still fire?
 *     Justify your choice with a one-line comment.
 */
public class Exercise3_AbandonedCart {

    private static final long TIMEOUT_MS = 5_000L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> clicks = env.fromElements(
                // u1: adds and purchases quickly  → NO alert
                new ClickEvent("u1", "p100", "electronics", 299.99, 1000L, "add_to_cart"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 1500L, "purchase"),

                // u2: adds and never purchases    → ALERT
                new ClickEvent("u2", "p200", "books", 19.50, 2000L, "add_to_cart"),

                // u3: adds, then views (not a purchase) → ALERT
                new ClickEvent("u3", "p300", "clothing", 49.00, 3000L, "add_to_cart"),
                new ClickEvent("u3", "p301", "clothing", 89.00, 3500L, "view"),

                // u4: purchases without ever adding → NO alert (no add_to_cart at all)
                new ClickEvent("u4", "p400", "books", 12.00, 4000L, "purchase"),

                // u5: adds, adds again, then purchases → NO alert
                new ClickEvent("u5", "p500", "clothing", 39.99, 5000L, "add_to_cart"),
                new ClickEvent("u5", "p501", "clothing", 59.99, 5500L, "add_to_cart"),
                new ClickEvent("u5", "p501", "clothing", 59.99, 6000L, "purchase")
        );

        DataStream<ClickEvent> withTimestamps = clicks.assignTimestampsAndWatermarks(             
          WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()                           
                  .withTimestampAssigner((event, recordTimestamp) -> event.timestamp)       
        );
        KeyedStream<ClickEvent, String> keyed = withTimestamps.keyBy(event -> event.getUserId());
        DataStream<String> alerts = keyed.process(new AbandonedCartAlert());

        alerts.print();

        env.execute("Exercise 3 - Abandoned Cart");
    }

    public static class AbandonedCartAlert extends KeyedProcessFunction<String, ClickEvent, String> {
        private ValueState<Long> cartTimerState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "car timer", Long.class
            );
            cartTimerState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) throws Exception {
            String action = event.getAction();

            if ("add_to_cart".equals(action)) {
                Long old = cartTimerState.value();          
                if (old != null) {                                                                        
                    ctx.timerService().deleteEventTimeTimer(old);               
                }
                long timerTime = ctx.timestamp() + TIMEOUT_MS;                                        
                ctx.timerService().registerEventTimeTimer(timerTime);
                cartTimerState.update(timerTime);
            } else if ("purchase".equals(action)) {
                Long currentTimer = cartTimerState.value();
                if (currentTimer != null) {
                    ctx.timerService().deleteEventTimeTimer(currentTimer);
                    cartTimerState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("ALERT: User " + ctx.getCurrentKey() + " abandoned their cart!");
            cartTimerState.clear();
        }
    }
}
