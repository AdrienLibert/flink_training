package com.training.flink.exercises;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Lesson 11 — Data Enrichment.
 *
 * The naive way to add a per-event lookup is `RichMapFunction` calling a
 * sync HTTP/JDBC client. Result: throughput drops to (1 / latency_per_call).
 * 50ms latency = 20 events/sec, no matter how big the cluster.
 *
 * Async I/O fixes this: launch many in-flight requests, complete them in
 * any order (or in arrival order), keep the operator thread free.
 *
 * Stage 1: implement {@link UserLookupAsyncFunction#asyncInvoke} so it
 *          fires a CompletableFuture lookup against {@code FAKE_DB}.
 * Stage 2: wire it into the pipeline with `AsyncDataStream.unorderedWait`
 *          using capacity 4 and 1-second timeout.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.AsyncEnrichment"
 */
public class AsyncEnrichment {

    private static final Map<String, String> FAKE_DB = Map.of(
            "u0", "Alice",
            "u1", "Bob",
            "u2", "Carol",
            "u3", "Dave");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> userIds = env.fromElements(
                "u0", "u1", "u2", "u3", "u0", "u1", "u2", "u3", "u0", "u1");

        DataStream<Tuple2<String, String>> enriched = enrich(userIds);
        enriched.print("ENRICHED");

        env.execute("Lesson 11 — Async Enrichment");
        System.out.println("[note] If output order differs from input order, async I/O is working.");
    }

    // ----- Stage 2 -----
    static DataStream<Tuple2<String, String>> enrich(DataStream<String> userIds) {
        // TODO: AsyncDataStream.unorderedWait(
        //         userIds, new UserLookupAsyncFunction(),
        //         1, TimeUnit.SECONDS, /*capacity=*/4)
        throw new UnsupportedOperationException("Implement enrich");
    }

    // ----- Stage 1 -----
    public static class UserLookupAsyncFunction
            extends RichAsyncFunction<String, Tuple2<String, String>> {

        private transient ExecutorService executor;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            executor = Executors.newFixedThreadPool(8);
        }

        @Override
        public void close() throws Exception {
            executor.shutdownNow();
        }

        @Override
        public void asyncInvoke(String userId, ResultFuture<Tuple2<String, String>> resultFuture) {
            // TODO: submit a lookup to `executor` that:
            //   1. sleeps 100ms (simulates network round-trip)
            //   2. resolves to FAKE_DB.getOrDefault(userId, "?")
            //   3. completes the resultFuture with Tuple2.of(userId, name).
            // Important: NEVER call resultFuture.complete on the operator
            // thread synchronously — that defeats async I/O. Always use a
            // separate executor or non-blocking client.
            throw new UnsupportedOperationException("Implement asyncInvoke");
        }

        @Override
        public void timeout(String input, ResultFuture<Tuple2<String, String>> resultFuture) {
            resultFuture.complete(Collections.singletonList(Tuple2.of(input, "?TIMEOUT?")));
        }
    }
}
