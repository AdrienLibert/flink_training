package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.util.PacedClickSource;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Exercise 3 — Solution: State TTL
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise3_StateTtl_Solution"
 *
 * Expected output (timings approximate):
 *   u1 | gap=…ms     | counter=1     ← first click, fresh state
 *   u1 | gap=1000ms  | counter=2     ← within TTL
 *   u1 | gap=5000ms  | counter=1     ← TTL=3s elapsed, state was expired,
 *                                       value() returned null, counter reset
 *   u1 | gap=1000ms  | counter=2     ← within TTL again
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   Why is lazy (read-time) cleanup a production problem, and how do you fix it?
 *
 * ANSWER:
 *   The problem:
 *     If a key is written once and never read again (e.g. a session that
 *     went silent), the entry stays in state forever. State size grows
 *     unboundedly even though logically all that data is "expired."
 *     This is one of the most common causes of unbounded RocksDB growth
 *     in production Flink jobs.
 *
 *   Three knobs StateTtlConfig gives you:
 *
 *   1. cleanupFullSnapshot()
 *        Expired entries are dropped when a full checkpoint or savepoint
 *        is taken. Cheap, but only helps when you actually take snapshots.
 *
 *   2. cleanupIncrementally(int cleanupSize, boolean runCleanupForEveryRecord)
 *        On the heap state backend. Each access scans `cleanupSize` extra
 *        entries and drops expired ones. Steady, predictable cleanup
 *        amortised across the workload.
 *
 *   3. cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries)
 *        On the RocksDB backend. Plugs a custom CompactionFilter into
 *        RocksDB so expired entries are dropped during background
 *        compactions. This is the standard recommendation for production
 *        Flink with RocksDB. Default is enabled in modern Flink versions
 *        but you'll want to TUNE the queryTime parameter for very-high-cardinality
 *        keyspaces.
 *
 *   Rule of thumb: never ship a TTL'd state to prod without explicitly
 *   choosing one of these strategies. Lazy cleanup alone is a footgun.
 * --------------------------------------------------------------------------
 */
public class Exercise3_StateTtl_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        env.execute("Exercise 3 - State TTL (solution)");
    }

    public static class SessionCounter
            extends KeyedProcessFunction<String, ClickEvent, String> {

        private transient ValueState<Long> counter;
        private transient long lastSeenWallMs;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig
                    .newBuilder(Time.seconds(3))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupIncrementally(10, true)
                    .build();

            ValueStateDescriptor<Long> desc =
                    new ValueStateDescriptor<>("counter", Long.class);
            desc.enableTimeToLive(ttl);

            counter = getRuntimeContext().getState(desc);
            lastSeenWallMs = System.currentTimeMillis();
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) throws Exception {
            long now = System.currentTimeMillis();
            long gap = now - lastSeenWallMs;
            lastSeenWallMs = now;

            Long current = counter.value();   // null if state expired or never written
            long next = (current == null ? 0L : current) + 1L;
            counter.update(next);

            out.collect(String.format("%s | gap=%dms | counter=%d %s",
                    event.userId, gap, next,
                    current == null ? "(fresh / expired)" : ""));
        }
    }
}
