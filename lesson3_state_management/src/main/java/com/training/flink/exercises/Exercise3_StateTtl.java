package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.PacedClickSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Exercise 3: State TTL (time-to-live)
 *
 * Goal:
 *   Configure ValueState with a TTL and OBSERVE expiry behaviour.
 *   We deliberately use processing-time delays so you can see state
 *   "go away" between events.
 *
 *   Build a per-user "session counter" that:
 *     - increments on each click
 *     - resets to 0 when the underlying state has expired
 *
 *   With a TTL of 3 seconds and the event timing below, you should see the
 *   counter RESET on the click that comes 5 seconds after the previous one.
 *
 * Concepts:
 *   - StateTtlConfig.newBuilder(Time.seconds(3))
 *   - UpdateType.OnCreateAndWrite — TTL resets only on writes
 *   - UpdateType.OnReadAndWrite   — TTL resets on reads too (sliding session)
 *   - StateVisibility.NeverReturnExpired — read returns null once TTL passed
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise3_StateTtl"
 *
 * Bonus question:
 *   Default cleanup of expired state is "lazy" — entries are removed only
 *   when read. Why is that potentially a problem for production? What
 *   options does StateTtlConfig give you to fix it?
 */
public class Exercise3_StateTtl {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 4 events for u1, all same key, paced in real time:
        //   t=0.5s   : click #1   (state created)
        //   t=1.5s   : click #2   (within TTL, count=2)
        //   t=6.5s   : click #3   (5s gap > 3s TTL → state expired, count resets to 1)
        //   t=7.5s   : click #4   (within TTL, count=2)
        ClickEvent[] events = {
                new ClickEvent("u1", "books", 10.0, 1L),
                new ClickEvent("u1", "books", 11.0, 2L),
                new ClickEvent("u1", "books", 12.0, 3L),
                new ClickEvent("u1", "books", 13.0, 4L)
        };
        long[] delays = {500, 1000, 5000, 1000};

        DataStream<ClickEvent> clicks =
                env.addSource(new PacedClickSource(events, delays));

        clicks
                .keyBy(c -> c.userId)
                .process(new SessionCounter())
                .print();

        env.execute("Exercise 3 - State TTL");
    }

    /**
     * TODO:
     *   1. Build a StateTtlConfig:
     *
     *        StateTtlConfig ttl = StateTtlConfig
     *            .newBuilder(Time.seconds(3))
     *            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
     *            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
     *            .build();
     *
     *   2. Build a ValueStateDescriptor&lt;Long&gt; named "counter" and call
     *      .enableTimeToLive(ttl) on it BEFORE handing it to getRuntimeContext().
     *
     *   3. In processElement():
     *        - read counter.value()
     *        - if null (either fresh key or expired), start at 0
     *        - increment, write back, and emit
     *          "userId | wallTime=… | counter=…"
     *        - use System.currentTimeMillis() so you can see the gap that
     *          triggered the reset.
     */
    public static class SessionCounter
            extends KeyedProcessFunction<String, ClickEvent, String> {

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) {
            // TODO replace this placeholder
            out.collect(event.userId + " | (TTL state not implemented yet)");
        }
    }
}
