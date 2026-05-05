package com.training.flink.solutions;

import com.training.flink.model.Alert;
import com.training.flink.model.ClickEvent;
import com.training.flink.model.EnrichedClick;
import com.training.flink.model.UserProfile;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Reference solution for {@code com.training.flink.exercises.EndToEndAnalytics}.
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.solutions.EndToEndAnalytics_Solution"
 *
 * See EXERCISES.md for the bonus-question answers.
 */
public class EndToEndAnalytics_Solution {

    public static final long ABANDONMENT_TIMEOUT_MS = 5_000L;
    public static final double HIGH_VALUE_THRESHOLD = 500.0;

    public static final OutputTag<ClickEvent> REGULAR_TAG =
            new OutputTag<ClickEvent>("regular") {};
    public static final OutputTag<ClickEvent> NON_PURCHASE_TAG =
            new OutputTag<ClickEvent>("non-purchase") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> rawClicks = env.fromElements(
                new ClickEvent("u1", "p100", "electronics", 299.99, 1000L, "view"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 1500L, "add_to_cart"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 2000L, "purchase"),
                new ClickEvent("u2", "p200", "books",        19.50, 2500L, "purchase"),
                new ClickEvent("u3", "p300", "electronics", 799.00, 3000L, "add_to_cart"),
                new ClickEvent("u3", "p300", "electronics", 799.00, 3500L, "purchase"),
                new ClickEvent("u4", "p400", "clothing",    120.00, 4000L, "add_to_cart"),
                new ClickEvent("u5", "p500", "electronics", 1299.00, 5000L, "purchase"),
                new ClickEvent("u6", "p600", "books",        25.00, 6000L, "purchase"),
                new ClickEvent("u7", "p700", "clothing",     89.99, 7000L, "view"),
                new ClickEvent("u8", "p800", "clothing",     49.00, 8000L, "add_to_cart")
        );

        DataStream<UserProfile> profiles = env.fromElements(
                new UserProfile("u1", "premium"),
                new UserProfile("u2", "free"),
                new UserProfile("u2", "vip"),
                new UserProfile("u3", "premium"),
                new UserProfile("u5", "vip")
        );

        DataStream<Tuple2<String, Double>> purchases = purchaseTuples(rawClicks);
        DataStream<Tuple2<String, Double>> revenue = revenuePerCategory(rawClicks);
        DataStream<Alert> alerts = abandonedCartAlerts(rawClicks);
        RoutedClicks routed = routeByValue(rawClicks);
        DataStream<EnrichedClick> enriched = enrichWithTier(rawClicks, profiles);

        purchases.print("PURCHASE-TUPLE");
        revenue.print("REVENUE");
        alerts.print("ABANDONED");
        routed.highValue.print("HIGH-VALUE");
        routed.regular.print("REGULAR");
        routed.nonPurchase.print("NON-PURCHASE");
        enriched.print("ENRICHED");

        env.execute("Lesson 1 - End-to-end analytics (solution)");
    }

    static DataStream<Tuple2<String, Double>> purchaseTuples(DataStream<ClickEvent> clicks) {
        return clicks
                .filter(e -> "purchase".equals(e.action))
                .map(e -> new Tuple2<>(e.category, e.price))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
    }

    static DataStream<Tuple2<String, Double>> revenuePerCategory(DataStream<ClickEvent> clicks) {
        return purchaseTuples(clicks)
                .keyBy(t -> t.f0)
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
    }

    static DataStream<Alert> abandonedCartAlerts(DataStream<ClickEvent> clicks) {
        DataStream<ClickEvent> withTimestamps = clicks.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.timestamp));

        return withTimestamps
                .keyBy(e -> e.userId)
                .process(new AbandonedCartDetector());
    }

    static RoutedClicks routeByValue(DataStream<ClickEvent> clicks) {
        SingleOutputStreamOperator<ClickEvent> highValue = clicks.process(
                new ProcessFunction<ClickEvent, ClickEvent>() {
                    @Override
                    public void processElement(ClickEvent event,
                                               Context ctx,
                                               Collector<ClickEvent> out) {
                        if ("purchase".equals(event.action)) {
                            if (event.price > HIGH_VALUE_THRESHOLD) {
                                out.collect(event);
                            } else {
                                ctx.output(REGULAR_TAG, event);
                            }
                        } else {
                            ctx.output(NON_PURCHASE_TAG, event);
                        }
                    }
                });

        return new RoutedClicks(
                highValue,
                highValue.getSideOutput(REGULAR_TAG),
                highValue.getSideOutput(NON_PURCHASE_TAG));
    }

    static DataStream<EnrichedClick> enrichWithTier(
            DataStream<ClickEvent> clicks, DataStream<UserProfile> profiles) {
        KeyedStream<ClickEvent, String> keyedClicks = clicks.keyBy(e -> e.userId);
        KeyedStream<UserProfile, String> keyedProfiles = profiles.keyBy(p -> p.userId);
        ConnectedStreams<ClickEvent, UserProfile> connected = keyedClicks.connect(keyedProfiles);
        return connected.process(new TierEnricher());
    }

    public static final class RoutedClicks {
        public final DataStream<ClickEvent> highValue;
        public final DataStream<ClickEvent> regular;
        public final DataStream<ClickEvent> nonPurchase;

        public RoutedClicks(DataStream<ClickEvent> highValue,
                            DataStream<ClickEvent> regular,
                            DataStream<ClickEvent> nonPurchase) {
            this.highValue = highValue;
            this.regular = regular;
            this.nonPurchase = nonPurchase;
        }
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
                Long previous = pendingTimer.value();
                if (previous != null) {
                    ctx.timerService().deleteEventTimeTimer(previous);
                }
                long fireAt = ctx.timestamp() + ABANDONMENT_TIMEOUT_MS;
                ctx.timerService().registerEventTimeTimer(fireAt);
                pendingTimer.update(fireAt);
            } else if ("purchase".equals(action)) {
                Long previous = pendingTimer.value();
                if (previous != null) {
                    ctx.timerService().deleteEventTimeTimer(previous);
                    pendingTimer.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out)
                throws Exception {
            out.collect(new Alert(ctx.getCurrentKey(), "abandoned cart"));
            pendingTimer.clear();
        }
    }

    public static class TierEnricher
            extends KeyedCoProcessFunction<String, ClickEvent, UserProfile, EnrichedClick> {

        private transient ValueState<String> tierState;

        @Override
        public void open(Configuration parameters) {
            tierState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("userTier", String.class));
        }

        @Override
        public void processElement1(ClickEvent click, Context ctx, Collector<EnrichedClick> out)
                throws Exception {
            String tier = tierState.value();
            out.collect(new EnrichedClick(click, tier != null ? tier : "unknown"));
        }

        @Override
        public void processElement2(UserProfile profile, Context ctx, Collector<EnrichedClick> out)
                throws Exception {
            tierState.update(profile.tier);
        }
    }
}
