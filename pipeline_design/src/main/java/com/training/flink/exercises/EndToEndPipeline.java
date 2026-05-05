package com.training.flink.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Lesson 14 — Workshop: Pipeline Design.
 *
 * The goal of this workshop is to apply the patterns from lessons
 * 10–13 in one place: a small but realistic pipeline that
 *
 *   - reads bounded "events" from a datagen source,
 *   - aggregates per category with a 5s tumbling window,
 *   - emits the result to an upsert-style sink (`upsert-kafka`-shaped)
 *     so downstream consumers can reconstruct the latest aggregate.
 *
 * We use the SQL planner since SQL is the natural expression of the
 * upsert/retract semantics at the heart of changelog pipelines.
 *
 * Stage 1: declare a `datagen` source for `(category, amount, ts)`.
 * Stage 2: declare a sink that PRIMARY KEY (category) — the planner
 *          recognizes this as a CHANGELOG sink and emits +U/-U.
 * Stage 3: write the aggregation INSERT and run.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.EndToEndPipeline"
 */
public class EndToEndPipeline {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        declareSource(tEnv);
        declareSink(tEnv);
        runAggregation(tEnv);
    }

    // ----- Stage 1 -----
    static void declareSource(StreamTableEnvironment tEnv) {
        // TODO: CREATE TEMPORARY TABLE events (
        //   category STRING,
        //   amount DOUBLE,
        //   event_time TIMESTAMP_LTZ(3),
        //   WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
        // ) WITH ( 'connector' = 'datagen', 'number-of-rows' = '60',
        //          'rows-per-second' = '20',
        //          'fields.category.kind' = 'random',
        //          'fields.category.length' = '1',  // 16 categories → overlaps
        //          'fields.amount.min' = '1.0',
        //          'fields.amount.max' = '50.0' )
        throw new UnsupportedOperationException("Stage 1 — declareSource");
    }

    // ----- Stage 2 -----
    static void declareSink(StreamTableEnvironment tEnv) {
        // TODO: CREATE TEMPORARY TABLE category_totals (
        //   category STRING,
        //   total DOUBLE,
        //   PRIMARY KEY (category) NOT ENFORCED
        // ) WITH ( 'connector' = 'print' )
        // The PRIMARY KEY clause makes this an upsert sink — the planner
        // will route +U/-U to the same key, ideal for an UPSERT into
        // Kafka, JDBC, or any downstream system that supports it.
        throw new UnsupportedOperationException("Stage 2 — declareSink");
    }

    // ----- Stage 3 -----
    static void runAggregation(StreamTableEnvironment tEnv) throws Exception {
        // TODO:
        //   tEnv.executeSql(
        //     "INSERT INTO category_totals " +
        //     "SELECT category, SUM(amount) " +
        //     "FROM events GROUP BY category"
        //   ).await();
        // Why GROUP BY without windowing? It produces a CHANGELOG: each
        // event triggers a +U for its category. The upsert sink collapses
        // these into a "current value" view downstream — the production
        // shape for "what's the current per-category total" dashboards.
        throw new UnsupportedOperationException("Stage 3 — runAggregation");
    }
}
