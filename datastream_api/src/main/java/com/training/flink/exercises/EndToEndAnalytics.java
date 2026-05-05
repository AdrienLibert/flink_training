package com.training.flink.exercises;

import com.training.flink.model.Alert;
import com.training.flink.model.ClickEvent;
import com.training.flink.model.EnrichedClick;
import com.training.flink.model.UserProfile;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.OutputTag;

/**
 * Lesson 1 — DataStream API: end-to-end e-commerce analytics.
 *
 * One big exercise broken into 5 stages. Each stage is a static method you
 * must implement. The {@link #main(String[])} method wires them together
 * into a single job that prints all six output channels.
 *
 * Recommended implementation order: Stage 1 → Stage 2 → Stage 3 → Stage 4 → Stage 5.
 * The skeleton compiles and runs as-is; whichever stage is not yet implemented
 * will throw {@link UnsupportedOperationException} when the job graph is built.
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.EndToEndAnalytics"
 *
 * See {@code EXERCISES.md} for the full per-stage spec and bonus questions.
 */
public class EndToEndAnalytics {

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

        env.execute("Lesson 1 - End-to-end analytics");
    }

    /**
     * Stage 1: keep only "purchase" events and map each to a {@code (category, price)} tuple.
     *
     * Skills: filter, map, .returns(...) for generic type recovery.
     *
     * Hint: Flink can't infer Tuple2 from a lambda because of Java type erasure —
     * you must call {@code .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))}.
     */
    static DataStream<Tuple2<String, Double>> purchaseTuples(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 1: filter purchase events and map to (category, price)");
    }

    /**
     * Stage 2: compute the running revenue total per category as a continuous stream.
     *
     * Skills: keyBy, reduce, stateful aggregation on a keyed stream.
     *
     * Hint: build on Stage 1 — call {@link #purchaseTuples(DataStream)} first,
     * then keyBy(category) and reduce by summing prices.
     *
     * Bonus question: why does the output show MULTIPLE totals for the same
     * category instead of one final number? (Answer in EXERCISES.md.)
     */
    static DataStream<Tuple2<String, Double>> revenuePerCategory(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 2: keyBy category and reduce to a running total");
    }

    /**
     * Stage 3: emit an {@link Alert} for every user who added to cart but did not
     * purchase within {@link #ABANDONMENT_TIMEOUT_MS} (event-time milliseconds).
     *
     * Skills: WatermarkStrategy, KeyedProcessFunction, ValueState, event-time timer.
     *
     * Hints:
     *   - Use {@code WatermarkStrategy.forMonotonousTimestamps()} on the source
     *     timestamps before keyBy.
     *   - Use ValueState<Long> to remember the last registered timer.
     *   - On a second add_to_cart, delete the previous timer and register a new one
     *     (reset semantics — engaged users should not be alerted).
     *   - On a purchase, cancel and clear state.
     *   - In onTimer, emit the alert and clear state.
     *
     * Implementation tip: implement {@link AbandonedCartDetector} below.
     */
    static DataStream<Alert> abandonedCartAlerts(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 3: detect abandoned carts via KeyedProcessFunction + event-time timer");
    }

    /**
     * Stage 4: route the input stream into three channels in a single pass:
     *   - main:                     purchases with price &gt; {@link #HIGH_VALUE_THRESHOLD}
     *   - side {@link #REGULAR_TAG}: purchases with price &lt;= threshold
     *   - side {@link #NON_PURCHASE_TAG}: any non-purchase event
     *
     * Skills: ProcessFunction, OutputTag, side outputs.
     *
     * Hint: the OutputTag MUST use the anonymous-subclass form
     * {@code new OutputTag<ClickEvent>("name") {}} so Flink can recover the
     * generic type at runtime — see the bonus question in EXERCISES.md.
     */
    static RoutedClicks routeByValue(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 4: split into high-value / regular / non-purchase via side outputs");
    }

    /**
     * Stage 5: enrich each ClickEvent with the user's current tier.
     *
     * Profiles are a slowly-changing reference stream. If a click arrives before
     * any profile for that user, emit tier = "unknown".
     *
     * Skills: connect, KeyedCoProcessFunction, ValueState.
     *
     * Hint: keyBy userId on BOTH streams, connect them, and implement
     * {@link TierEnricher} below.
     *
     * Bonus question: when would BroadcastState be a better fit than connect+keyBy?
     * (Answer in EXERCISES.md.)
     */
    static DataStream<EnrichedClick> enrichWithTier(
            DataStream<ClickEvent> clicks, DataStream<UserProfile> profiles) {
        throw new UnsupportedOperationException(
                "Stage 5: connect clicks + profiles and enrich via KeyedCoProcessFunction");
    }

    /** Holder for the three streams produced by Stage 4. */
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

    /** Stage 3 helper. Implement processElement() and onTimer(). */
    public static class AbandonedCartDetector
            extends KeyedProcessFunction<String, ClickEvent, Alert> {
        // TODO Stage 3:
        //   - declare a transient ValueState<Long> pendingTimer
        //   - override open(Configuration) to initialize the state descriptor
        //   - replace processElement() with the add_to_cart / purchase logic
        //   - override onTimer() to emit the Alert and clear state

        @Override
        public void processElement(ClickEvent event,
                                   Context ctx,
                                   org.apache.flink.util.Collector<Alert> out) throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 3: implement AbandonedCartDetector.processElement()");
        }
    }

    /** Stage 5 helper. Implement processElement1 (clicks) and processElement2 (profiles). */
    public static class TierEnricher
            extends KeyedCoProcessFunction<String, ClickEvent, UserProfile, EnrichedClick> {
        // TODO Stage 5:
        //   - declare a transient ValueState<String> tierState
        //   - override open(Configuration) to initialize the state descriptor
        //   - replace processElement1: emit EnrichedClick using current tier (or "unknown")
        //   - replace processElement2: update tierState from the incoming profile (no emission)

        @Override
        public void processElement1(ClickEvent click,
                                    Context ctx,
                                    org.apache.flink.util.Collector<EnrichedClick> out) throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 5: implement TierEnricher.processElement1()");
        }

        @Override
        public void processElement2(UserProfile profile,
                                    Context ctx,
                                    org.apache.flink.util.Collector<EnrichedClick> out) throws Exception {
            throw new UnsupportedOperationException(
                    "Stage 5: implement TierEnricher.processElement2()");
        }
    }
}
