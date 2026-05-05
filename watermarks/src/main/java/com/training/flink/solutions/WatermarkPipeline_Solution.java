package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.PacedClickSource;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Reference solution for {@code com.training.flink.exercises.WatermarkPipeline}.
 *
 * Run:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.solutions.WatermarkPipeline_Solution"
 */
public class WatermarkPipeline_Solution {

    public static final long OUT_OF_ORDERNESS_MS = 2_000L;
    public static final long WINDOW_SIZE_MS = 5_000L;
    public static final long ALLOWED_LATENESS_MS = 2_000L;

    public static final OutputTag<ClickEvent> LATE_EVENTS_TAG =
            new OutputTag<ClickEvent>("late") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ClickEvent[] events = {
                new ClickEvent("u1", "books",       12.0, 1_000L),
                new ClickEvent("u2", "electronics", 100.0, 1_500L),
                new ClickEvent("u1", "books",        8.0, 2_500L),
                new ClickEvent("u3", "books",       15.0, 4_500L),
                new ClickEvent("u4", "books",       20.0, 4_000L),
                new ClickEvent("u1", "electronics", 250.0, 5_500L),
                new ClickEvent("u2", "books",       11.0, 6_000L),
                new ClickEvent("u3", "electronics", 800.0, 8_000L),
                new ClickEvent("u5", "books",        3.0, 3_000L),
                new ClickEvent("u7", "electronics", 50.0, 9_500L),
                new ClickEvent("u6", "books",       99.0, 1_000L),
                new ClickEvent("u1", "books",        7.0, 11_000L),
                new ClickEvent("u2", "books",       13.0, 13_500L)
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

        env.execute("Lesson 4 - Watermark pipeline (solution)");
    }

    static DataStream<ClickEvent> withBoundedOutOfOrderness(DataStream<ClickEvent> clicks) {
        return clicks.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(
                                Duration.ofMillis(OUT_OF_ORDERNESS_MS))
                        .withTimestampAssigner((event, ts) -> event.timestamp));
    }

    static DataStream<Tuple3<String, Long, Double>> revenuePerCategoryWindowed(
            DataStream<ClickEvent> clicks) {
        return clicks
                .keyBy(c -> c.category)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
                .aggregate(new SumPriceAccumulator(), new EmitWindowSum());
    }

    static WindowedWithLate revenueWithAllowedLateness(DataStream<ClickEvent> clicks) {
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> windowed = clicks
                .keyBy(c -> c.category)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
                .allowedLateness(Time.milliseconds(ALLOWED_LATENESS_MS))
                .sideOutputLateData(LATE_EVENTS_TAG)
                .aggregate(new SumPriceAccumulator(), new EmitWindowSum());

        return new WindowedWithLate(windowed, windowed.getSideOutput(LATE_EVENTS_TAG));
    }

    static DataStream<ClickEvent> customWithIdleness(DataStream<ClickEvent> clicks) {
        WatermarkStrategy<ClickEvent> strategy = WatermarkStrategy
                .<ClickEvent>forGenerator(new PunctuatedSupplier())
                .withTimestampAssigner((event, ts) -> event.timestamp)
                .withIdleness(Duration.ofMillis(2_000));
        return clicks.assignTimestampsAndWatermarks(strategy);
    }

    public static final class WindowedWithLate {
        public final DataStream<Tuple3<String, Long, Double>> results;
        public final DataStream<ClickEvent> lateEvents;

        public WindowedWithLate(DataStream<Tuple3<String, Long, Double>> results,
                                DataStream<ClickEvent> lateEvents) {
            this.results = results;
            this.lateEvents = lateEvents;
        }
    }

    /** Tiny sum accumulator that keeps the running total in a Double. */
    public static class SumPriceAccumulator implements AggregateFunction<ClickEvent, Double, Double> {
        @Override public Double createAccumulator() { return 0.0; }
        @Override public Double add(ClickEvent value, Double acc) { return acc + value.price; }
        @Override public Double getResult(Double acc) { return acc; }
        @Override public Double merge(Double a, Double b) { return a + b; }
    }

    /** Tags the aggregate result with category + window-end. */
    public static class EmitWindowSum
            extends ProcessWindowFunction<Double, Tuple3<String, Long, Double>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Double> elements,
                            Collector<Tuple3<String, Long, Double>> out) {
            Double total = elements.iterator().next();
            out.collect(new Tuple3<>(key, ctx.window().getEnd(), total));
        }
    }

    public static class PunctuatedOnPurchase implements WatermarkGenerator<ClickEvent> {
        @Override
        public void onEvent(ClickEvent event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(eventTimestamp - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    public static class PunctuatedSupplier implements WatermarkGeneratorSupplier<ClickEvent> {
        @Override
        public WatermarkGenerator<ClickEvent> createWatermarkGenerator(Context ctx) {
            return new PunctuatedOnPurchase();
        }
    }
}
