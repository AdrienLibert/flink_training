package com.training.flink.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Lesson 7 — Workshop: SQL Query Evolution.
 *
 * The goal is to build intuition for which SQL changes ARE state-safe
 * and which are NOT, by compiling each variant and inspecting the plan.
 * For each query variant V we:
 *   1. compile its plan to JSON
 *   2. extract the operator IDs and types
 *   3. compare to baseline — same operator IDs / types means restore is OK
 *
 * Run with:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.QueryEvolutionLab"
 */
public class QueryEvolutionLab {

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

    /** Variants to compile. Map of label -> SQL (must each be a complete INSERT). */
    private static final Map<String, String> VARIANTS = baselineVariants();

    private static Map<String, String> baselineVariants() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("V0_baseline",
                "INSERT INTO sink_kv "
                        + "SELECT category, SUM(amount) "
                        + "FROM orders GROUP BY category");
        m.put("V1_safe_projection",
                // Same GROUP BY key, just multiplied output — state-safe.
                "INSERT INTO sink_kv "
                        + "SELECT category, SUM(amount) * 1.0 "
                        + "FROM orders GROUP BY category");
        m.put("V2_safe_filter_after_agg",
                // Filter on the OUTPUT — calc op added downstream of the agg, state-safe.
                "INSERT INTO sink_kv "
                        + "SELECT category, total FROM ("
                        + "  SELECT category, SUM(amount) AS total "
                        + "  FROM orders GROUP BY category"
                        + ") WHERE total > 1.0");
        m.put("V3_break_filter_before_agg",
                // Filter on the INPUT to the agg — accumulator state diverges, BREAKING.
                "INSERT INTO sink_kv "
                        + "SELECT category, SUM(amount) "
                        + "FROM orders WHERE amount > 5.0 GROUP BY category");
        m.put("V4_break_extra_groupby_key",
                // New group key — keying scheme changes, BREAKING.
                "INSERT INTO sink_kkv "
                        + "SELECT category, userId, SUM(amount) "
                        + "FROM orders GROUP BY category, userId");
        return m;
    }

    public static void main(String[] args) throws Exception {
        // Stage 1: compile each variant to JSON, write to /tmp.
        Map<String, Path> planFiles = compileAll(VARIANTS);

        // Stage 2: print a per-variant summary table.
        // For each plan, count nodes and surface the GroupAggregate node IDs.
        printPlanSummary(planFiles);
    }

    // ----- Stage 1 -----
    static Map<String, Path> compileAll(Map<String, String> variants) throws IOException {
        // TODO: for each (label, sql) build a fresh StreamTableEnvironment,
        //       executeSql(CREATE_SOURCE + appropriate sink), compilePlanSql(sql),
        //       writeToFile to /tmp/<label>.json. Return label->Path.
        throw new UnsupportedOperationException("Implement compileAll");
    }

    // ----- Stage 2 -----
    static void printPlanSummary(Map<String, Path> planFiles) throws IOException {
        // TODO: for each plan, read the JSON as text, print:
        //   - file size
        //   - number of "type" : "..." occurrences (a rough proxy for operator count)
        //   - whether the JSON contains '"GroupAggregate"' (the stateful op we care about)
        // The real demo: V3 and V4 introduce different GroupAggregate state schemas vs V0.
        throw new UnsupportedOperationException("Implement printPlanSummary");
    }

    static StreamTableEnvironment newTEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }
}
