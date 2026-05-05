package com.training.flink.solutions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryEvolutionLab_Solution {

    private static final String CREATE_SOURCE =
            "CREATE TEMPORARY TABLE orders (\n"
                    + "  orderId STRING,\n"
                    + "  userId STRING,\n"
                    + "  category STRING,\n"
                    + "  amount DOUBLE,\n"
                    + "  ts BIGINT\n"
                    + ") WITH (\n"
                    + "  'connector' = 'datagen',\n"
                    + "  'number-of-rows' = '5'\n"
                    + ")";

    private static final String CREATE_SINK_KV =
            "CREATE TEMPORARY TABLE sink_kv (k STRING, v DOUBLE) "
                    + "WITH ('connector' = 'print')";

    private static final String CREATE_SINK_KKV =
            "CREATE TEMPORARY TABLE sink_kkv (k1 STRING, k2 STRING, v DOUBLE) "
                    + "WITH ('connector' = 'print')";

    private static final Map<String, String> VARIANTS = variants();

    private static Map<String, String> variants() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("V0_baseline",
                "INSERT INTO sink_kv "
                        + "SELECT category, SUM(amount) "
                        + "FROM orders GROUP BY category");
        m.put("V1_safe_projection",
                "INSERT INTO sink_kv "
                        + "SELECT category, SUM(amount) * 1.0 "
                        + "FROM orders GROUP BY category");
        m.put("V2_safe_filter_after_agg",
                "INSERT INTO sink_kv "
                        + "SELECT category, total FROM ("
                        + "  SELECT category, SUM(amount) AS total "
                        + "  FROM orders GROUP BY category"
                        + ") WHERE total > 1.0");
        m.put("V3_break_filter_before_agg",
                "INSERT INTO sink_kv "
                        + "SELECT category, SUM(amount) "
                        + "FROM orders WHERE amount > 5.0 GROUP BY category");
        m.put("V4_break_extra_groupby_key",
                "INSERT INTO sink_kkv "
                        + "SELECT category, userId, SUM(amount) "
                        + "FROM orders GROUP BY category, userId");
        return m;
    }

    public static void main(String[] args) throws Exception {
        Map<String, Path> planFiles = compileAll(VARIANTS);
        printPlanSummary(planFiles);
    }

    static Map<String, Path> compileAll(Map<String, String> variants) throws IOException {
        Map<String, Path> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : variants.entrySet()) {
            StreamTableEnvironment tEnv = newTEnv();
            tEnv.executeSql(CREATE_SOURCE);
            tEnv.executeSql(CREATE_SINK_KV);
            tEnv.executeSql(CREATE_SINK_KKV);
            CompiledPlan plan = tEnv.compilePlanSql(e.getValue());
            Path file = Files.createTempFile("plan-" + e.getKey() + "-", ".json");
            Files.deleteIfExists(file);
            file.toFile().deleteOnExit();
            plan.writeToFile(file.toFile(), false);
            out.put(e.getKey(), file);
        }
        return out;
    }

    static void printPlanSummary(Map<String, Path> planFiles) throws IOException {
        System.out.printf("%-32s | %8s | %5s | %s%n",
                "variant", "bytes", "nodes", "has GroupAggregate");
        System.out.println("-".repeat(80));
        for (Map.Entry<String, Path> e : planFiles.entrySet()) {
            String json = Files.readString(e.getValue());
            int nodeCount = countOccurrences(json, "\"type\" :");
            boolean hasAgg = json.contains("GroupAggregate");
            System.out.printf("%-32s | %8d | %5d | %s%n",
                    e.getKey(), json.length(), nodeCount, hasAgg);
        }
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    static StreamTableEnvironment newTEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }
}
