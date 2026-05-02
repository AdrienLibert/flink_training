package com.training.flink.exercises;

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
 * Exercise 5: Stream Enrichment via connect() + KeyedCoProcessFunction
 *
 * Goal:
 *   Enrich every ClickEvent with the user's tier ("free", "premium", "vip"),
 *   pulled from a slowly-changing UserProfile stream.
 *
 *   For each click, emit an EnrichedClick(click, tier).
 *   - If we have not yet seen a profile for the user, emit tier = "unknown".
 *   - When a new profile arrives, update state for that user. Subsequent clicks
 *     should use the latest tier.
 */
public class Exercise5_StreamEnrichment {

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

        // TODO 1: keyBy userId on both streams.
        //   KeyedStream<ClickEvent,  String> keyedClicks   = ...
        //   KeyedStream<UserProfile, String> keyedProfiles = ...

        // TODO 2: connect + process with a KeyedCoProcessFunction.
        //   ConnectedStreams<ClickEvent, UserProfile> connected = ...
        //   DataStream<EnrichedClick> enriched = connected.process(new TierEnricher());

        // TODO 3: enriched.print();

        env.execute("Exercise 5 - Stream Enrichment");
    }

    // TODO 4: implement TierEnricher
    //
    //   public static class TierEnricher
    //       extends KeyedCoProcessFunction<String, ClickEvent, UserProfile, EnrichedClick> {
    //
    //       private ValueState<String> tierState;
    //
    //       @Override
    //       public void open(Configuration parameters) { ... }
    //
    //       @Override
    //       public void processElement1(ClickEvent click,    Context ctx, Collector<EnrichedClick> out) { ... }
    //
    //       @Override
    //       public void processElement2(UserProfile profile, Context ctx, Collector<EnrichedClick> out) { ... }
    //   }
}
