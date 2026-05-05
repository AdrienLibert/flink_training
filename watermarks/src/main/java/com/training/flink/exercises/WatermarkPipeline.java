package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.PacedClickSource;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * Lesson 4 — Custom Watermark Strategies: a single event-time analytics
 * pipeline that progresses from "the basics work" to "the fancy bits work."
 *
 * One big exercise broken into 4 stages. Each is a static method you must
 * implement. {@link #main(String[])} wires them together and prints under
 * distinct labels.
 *
 * Recommended order: Stage 1 → Stage 2 → Stage 3 → Stage 4. The class
 * compiles and {@code main()} runs as-is — whichever stage is unimplemented
 * throws {@link UnsupportedOperationException} when its subgraph is built.
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.WatermarkPipeline"
 *
 * See {@code EXERCISES.md} for the per-stage spec and bonus questions.
 */
public class WatermarkPipeline {

    public static final long OUT_OF_ORDERNESS_MS = 2_000L;
    public static final long WINDOW_SIZE_MS = 5_000L;
    public static final long ALLOWED_LATENESS_MS = 2_000L;

    public static final OutputTag<ClickEvent> LATE_EVENTS_TAG =
            new OutputTag<ClickEvent>("late") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Paced source: events arrive with real processing-time gaps so the
        // periodic watermark generator (forBoundedOutOfOrderness) has time to
        // tick between events. Without this, fromElements flushes everything
        // in milliseconds and "late" events never look late.
        ClickEvent[] events = {
                // Window [0, 5000)
                new ClickEvent("u1", "books",       12.0, 1_000L),
                new ClickEvent("u2", "electronics", 100.0, 1_500L),
                new ClickEvent("u1", "books",        8.0, 2_500L),
                new ClickEvent("u3", "books",       15.0, 4_500L),
                // OUT-OF-ORDER (within the 2s allowance) — should still land in window 1
                new ClickEvent("u4", "books",       20.0, 4_000L),

                // Window [5000, 10000)
                new ClickEvent("u1", "electronics", 250.0, 5_500L),
                new ClickEvent("u2", "books",       11.0, 6_000L),
                new ClickEvent("u3", "electronics", 800.0, 8_000L),

                // LATE EVENT for window [0, 5000): watermark by now is past
                // 8_000 - 2_000 = 6_000. Within allowed lateness (2s, i.e.
                // until watermark > 6_999) → triggers an UPDATE for window 1.
                new ClickEvent("u5", "books", 3.0, 3_000L),

                // Watermark-pusher: lifts the watermark to 9_500 - 2_000 =
                // 7_500 so that window [0, 5000) is finally retired (its
                // late-grace window ends at watermark > 6_999).
                new ClickEvent("u7", "electronics", 50.0, 9_500L),

                // VERY LATE EVENT: watermark is now 7_500, past 7_000 →
                // window 1 has been cleaned up → routed to LATE-SIDE.
                new ClickEvent("u6", "books", 99.0, 1_000L),

                // Window [10000, 15000)
                new ClickEvent("u1", "books",       7.0, 11_000L),
                new ClickEvent("u2", "books",      13.0, 13_500L)
        };
        long[] delays = new long[events.length];
        java.util.Arrays.fill(delays, 600L);
        DataStream<ClickEvent> rawClicks = env.addSource(new PacedClickSource(events, delays));

        DataStream<ClickEvent> withWatermarks = withBoundedOutOfOrderness(rawClicks);
        DataStream<Tuple3<String, Long, Double>> windowed = revenuePerCategoryWindowed(withWatermarks);
        WindowedWithLate windowedLate = revenueWithAllowedLateness(withWatermarks);
        DataStream<ClickEvent> idleAware = customWithIdleness(rawClicks);

        withWatermarks.print("WITH-WATERMARKS");
        windowed.print("WINDOW");
        windowedLate.results.print("WINDOW-LATE");
        windowedLate.lateEvents.print("LATE-SIDE");
        idleAware.print("IDLE-AWARE");

        env.execute("Lesson 4 - Watermark pipeline");
    }

    /**
     * Stage 1: assign event-time timestamps and a watermark strategy that
     * tolerates {@link #OUT_OF_ORDERNESS_MS} of out-of-orderness.
     *
     * Skills: {@code WatermarkStrategy.forBoundedOutOfOrderness},
     * timestamp assigner.
     *
     * Hint:
     * {@code clicks.assignTimestampsAndWatermarks(
     *     WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofMillis(OUT_OF_ORDERNESS_MS))
     *         .withTimestampAssigner((event, ts) -> event.timestamp));}
     */
    static DataStream<ClickEvent> withBoundedOutOfOrderness(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 1: assign timestamps + bounded-out-of-orderness watermark");
    }

    /**
     * Stage 2: tumbling event-time windows of {@link #WINDOW_SIZE_MS}, keyed
     * by category. Emit {@code (category, windowEnd, totalRevenue)}.
     *
     * Skills: {@code TumblingEventTimeWindows}, {@code ProcessWindowFunction}
     * or {@code aggregate}.
     *
     * Hint: keyBy(category), then window with
     * {@code TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS))},
     * and reduce/aggregate to a sum. Consider using {@code .sum()} on a
     * mapped tuple, or {@code .aggregate(...)} with a tiny accumulator.
     */
    static DataStream<Tuple3<String, Long, Double>> revenuePerCategoryWindowed(
            DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 2: tumbling event-time window per category, sum prices");
    }

    /**
     * Stage 3: same windowed aggregation as Stage 2, but with
     * {@code allowedLateness({@link #ALLOWED_LATENESS_MS})} and a side output
     * for events that arrive past even that grace period.
     *
     * Skills: {@code allowedLateness}, {@code sideOutputLateData},
     * window updates after late events.
     *
     * Return both streams in a {@link WindowedWithLate} holder.
     *
     * Hint: after the window operator, call
     * {@code .getSideOutput(LATE_EVENTS_TAG)} on the result to retrieve the
     * late-event side stream.
     */
    static WindowedWithLate revenueWithAllowedLateness(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 3: window with allowed lateness + side output for very-late events");
    }

    /**
     * Stage 4: use a custom {@link WatermarkStrategy} with
     * {@code .withIdleness(Duration)} so that an idle parallel source does
     * not stall watermarks for the rest of the job.
     *
     * Build the watermark strategy inline (or with the supplied
     * {@link PunctuatedOnPurchase} generator below) and combine it with
     * {@code .withIdleness(Duration.ofMillis(2_000))}. Apply with
     * {@code assignTimestampsAndWatermarks}.
     *
     * Even with parallelism = 1 the job runs; the value of this stage is
     * understanding the API and *why* idleness exists. See EXERCISES.md.
     */
    static DataStream<ClickEvent> customWithIdleness(DataStream<ClickEvent> clicks) {
        throw new UnsupportedOperationException(
                "Stage 4: build a custom WatermarkStrategy with withIdleness(...)");
    }

    /** Holder for Stage 3's two output streams. */
    public static final class WindowedWithLate {
        public final DataStream<Tuple3<String, Long, Double>> results;
        public final DataStream<ClickEvent> lateEvents;

        public WindowedWithLate(DataStream<Tuple3<String, Long, Double>> results,
                                DataStream<ClickEvent> lateEvents) {
            this.results = results;
            this.lateEvents = lateEvents;
        }
    }

    /**
     * Stage 4 helper: a minimal punctuated {@link WatermarkGenerator} that
     * advances the watermark every time it sees a "purchase"-shaped event.
     * Wire it via {@code WatermarkStrategy.forGenerator(ctx -> new PunctuatedOnPurchase())}
     * if you want a non-trivial example to combine with {@code withIdleness}.
     */
    public static class PunctuatedOnPurchase implements WatermarkGenerator<ClickEvent> {
        @Override
        public void onEvent(ClickEvent event, long eventTimestamp,
                            WatermarkOutput output) {
            // Emit a watermark immediately on every event using event time.
            // Real punctuated generators usually look at a flag in the payload.
            output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(eventTimestamp - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Punctuated → nothing on the periodic tick.
        }
    }

    /** Convenience supplier for {@link PunctuatedOnPurchase}. */
    public static class PunctuatedSupplier
            implements WatermarkGeneratorSupplier<ClickEvent> {
        @Override
        public WatermarkGenerator<ClickEvent> createWatermarkGenerator(Context ctx) {
            return new PunctuatedOnPurchase();
        }
    }
}
