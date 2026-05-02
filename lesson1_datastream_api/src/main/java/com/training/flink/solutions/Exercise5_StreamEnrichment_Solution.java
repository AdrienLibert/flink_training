package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import com.training.flink.model.EnrichedClick;
import com.training.flink.model.UserProfile;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Exercise 5 — Solution: Stream Enrichment via connect() + KeyedCoProcessFunction
 *
 * Enrich each ClickEvent with the user's tier from a slowly-changing
 * UserProfile stream. If a click arrives before any profile, emit "unknown".
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise5_StreamEnrichment_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   When would you use BroadcastState instead of connect+keyBy?
 *
 * ANSWER:
 *   The dividing line is CARDINALITY of the reference stream.
 *
 *   Use connect + keyBy (this exercise) when:
 *     - The reference stream is HIGH-cardinality (many keys, e.g. millions
 *       of users). State must be partitioned, or no single TaskManager has
 *       enough memory to hold it all.
 *     - You only need each reference value at the matching keyed subtask.
 *     - Example: per-user profiles, per-session tokens, per-device config.
 *
 *   Use BroadcastState (`stream.broadcast(MapStateDescriptor) +
 *   BroadcastProcessFunction`) when:
 *     - The reference stream is LOW-cardinality (a small lookup table:
 *       hundreds to maybe low thousands of entries).
 *     - EVERY subtask needs the FULL table — typically because the join key
 *       on the main stream isn't the same as the lookup key (e.g. enrich a
 *       click stream keyed by userId with a category-rules map keyed by
 *       categoryId).
 *     - Example: feature flags, rule engines, currency conversion tables,
 *       enum-style category metadata, fraud rules.
 *
 *   Concrete answer to the hint:
 *     - 50M users → connect+keyBy (broadcasting 50M rows to every subtask
 *       blows up memory and bandwidth).
 *     - 200 enum-like categories → broadcast (cheap to replicate, every
 *       subtask gets the full map and can join without re-shuffling the
 *       main stream).
 *
 *   Both forms are SOFT JOINS with state — the join is eventually consistent.
 *   The race condition (click arriving before its profile) is inherent to
 *   both. To handle it strictly, see ExerciseStretch ideas: buffer clicks
 *   until profile arrives, or use event-time alignment.
 * --------------------------------------------------------------------------
 *
 * Race condition note:
 *   connect() does NOT order the two streams against each other. The order
 *   in which clicks vs profiles arrive at the operator is non-deterministic.
 *   So u1's first click may be enriched as "unknown" or "premium" depending
 *   on the race. This is real — production code addresses it by buffering
 *   clicks until the profile arrives, or by using event-time + watermarks
 *   to align the two streams.
 */
public class Exercise5_StreamEnrichment_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "p100", "electronics", 299.99, 1000L, "view"),
                new ClickEvent("u2", "p200", "books",        19.50, 2000L, "purchase"),
                new ClickEvent("u1", "p100", "electronics", 299.99, 5000L, "purchase"),
                new ClickEvent("u3", "p300", "clothing",     49.00, 6000L, "view"),
                new ClickEvent("u2", "p500", "clothing",     89.99, 9000L, "purchase")
        );

        DataStream<UserProfile> profiles = env.fromElements(
                new UserProfile("u1", "premium"),
                new UserProfile("u2", "free"),
                new UserProfile("u2", "vip")
        );

        KeyedStream<ClickEvent,  String> keyedClicks   = clicks.keyBy(e -> e.userId);
        KeyedStream<UserProfile, String> keyedProfiles = profiles.keyBy(p -> p.userId);

        ConnectedStreams<ClickEvent, UserProfile> connected = keyedClicks.connect(keyedProfiles);

        DataStream<EnrichedClick> enriched = connected.process(new TierEnricher());

        enriched.print();

        env.execute("Exercise 5 - Stream Enrichment (solution)");
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
            // Intentionally no emission — profile updates are reference data.
        }
    }
}
