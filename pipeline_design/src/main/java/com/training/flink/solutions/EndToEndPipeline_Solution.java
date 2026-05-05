package com.training.flink.solutions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EndToEndPipeline_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        declareSource(tEnv);
        declareSink(tEnv);
        runAggregation(tEnv);
    }

    static void declareSource(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE events (\n"
                        + "  category STRING,\n"
                        + "  amount DOUBLE,\n"
                        + "  event_time AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '60',\n"
                        + "  'rows-per-second' = '20',\n"
                        + "  'fields.category.kind' = 'random',\n"
                        + "  'fields.category.length' = '1',\n"
                        + "  'fields.amount.min' = '1.0',\n"
                        + "  'fields.amount.max' = '50.0'\n"
                        + ")");
    }

    static void declareSink(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE category_totals (\n"
                        + "  category STRING,\n"
                        + "  total DOUBLE,\n"
                        + "  PRIMARY KEY (category) NOT ENFORCED\n"
                        + ") WITH ('connector' = 'print')");
    }

    static void runAggregation(StreamTableEnvironment tEnv) throws Exception {
        tEnv.executeSql(
                "INSERT INTO category_totals "
                        + "SELECT category, SUM(amount) "
                        + "FROM events GROUP BY category"
        ).await();
    }
}
