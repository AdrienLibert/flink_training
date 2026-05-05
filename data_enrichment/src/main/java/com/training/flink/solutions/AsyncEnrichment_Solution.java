package com.training.flink.solutions;

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

public class AsyncEnrichment_Solution {

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

        env.execute("Lesson 11 — Async Enrichment (solution)");
        System.out.println("[note] If output order differs from input order, async I/O is working.");
    }

    static DataStream<Tuple2<String, String>> enrich(DataStream<String> userIds) {
        return AsyncDataStream.unorderedWait(
                userIds, new UserLookupAsyncFunction(),
                1, TimeUnit.SECONDS, 4);
    }

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
            CompletableFuture
                    .supplyAsync(() -> {
                        try {
                            Thread.sleep(100); // simulate network call
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return FAKE_DB.getOrDefault(userId, "?");
                    }, executor)
                    .thenAccept(name -> resultFuture.complete(
                            Collections.singletonList(Tuple2.of(userId, name))));
        }

        @Override
        public void timeout(String input, ResultFuture<Tuple2<String, String>> resultFuture) {
            resultFuture.complete(Collections.singletonList(Tuple2.of(input, "?TIMEOUT?")));
        }
    }
}
