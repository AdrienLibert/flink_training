package com.training.flink.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Lesson 6 — Compiled Plans & Table State Evolution.
 *
 * Stage 1: declare a source + a `print` sink, compile a SQL pipeline to a
 *          JSON plan and write it to disk.
 * Stage 2: load the same JSON plan and execute it (no recompilation).
 * Stage 3: tweak the SQL — adding a WHERE filter — and compile a new plan;
 *          confirm the JSON differs.
 * Stage 4 (theory): which mutations preserve state compatibility, which break it.
 *
 * Run with:
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.CompiledPlanLab"
 */
public class CompiledPlanLab {

    private static final String CREATE_SOURCE =
            "CREATE TEMPORARY TABLE orders_src (\n"
                    + "  orderId STRING,\n"
                    + "  userId STRING,\n"
                    + "  category STRING,\n"
                    + "  amount DOUBLE\n"
                    + ") WITH (\n"
                    + "  'connector' = 'datagen',\n"
                    + "  'number-of-rows' = '20',\n"
                    + "  'fields.category.kind' = 'random',\n"
                    + "  'fields.category.length' = '4',\n"
                    + "  'fields.amount.min' = '1.0',\n"
                    + "  'fields.amount.max' = '50.0'\n"
                    + ")";

    private static final String CREATE_SINK =
            "CREATE TEMPORARY TABLE totals_sink (\n"
                    + "  category STRING,\n"
                    + "  revenue DOUBLE\n"
                    + ") WITH ('connector' = 'print')";

    private static final String INSERT_BASELINE =
            "INSERT INTO totals_sink "
                    + "SELECT category, SUM(amount) "
                    + "FROM orders_src GROUP BY category";

    private static final String INSERT_FILTERED =
            "INSERT INTO totals_sink "
                    + "SELECT category, SUM(amount) "
                    + "FROM orders_src WHERE amount > 25.0 GROUP BY category";

    public static void main(String[] args) throws Exception {
        Path planFile = Files.createTempFile("plan-baseline", ".json");
        Files.deleteIfExists(planFile); // writeToFile refuses to overwrite an existing file
        planFile.toFile().deleteOnExit();

        // Stage 1
        compileBaseline(planFile);
        System.out.println("[Stage 1] wrote plan: " + planFile + " (" + Files.size(planFile) + " bytes)");

        // Stage 2
        executeFromFile(planFile);

        // Stage 3
        Path planFileFiltered = Files.createTempFile("plan-filtered", ".json");
        Files.deleteIfExists(planFileFiltered);
        planFileFiltered.toFile().deleteOnExit();
        compileFiltered(planFileFiltered);
        long baselineSize = Files.size(planFile);
        long filteredSize = Files.size(planFileFiltered);
        System.out.println("[Stage 3] baseline plan = " + baselineSize
                + " bytes, filtered plan = " + filteredSize
                + " bytes. Plans differ: " + (baselineSize != filteredSize));
    }

    // ----- Stage 1 -----
    static void compileBaseline(Path planFile) throws IOException {
        // TODO: build a StreamTableEnvironment, exec CREATE_SOURCE + CREATE_SINK,
        //       then call tEnv.compilePlanSql(INSERT_BASELINE) and
        //       plan.writeToFile(planFile.toFile(), false).
        // Gotcha: the second arg is `ignoreIfExists`, NOT `overwrite`.
        // Pass `false` to overwrite (or rely on Files.deleteIfExists in main).
        throw new UnsupportedOperationException("Implement Stage 1 — compileBaseline");
    }

    // ----- Stage 2 -----
    static void executeFromFile(Path planFile) throws Exception {
        // TODO: build a StreamTableEnvironment, exec CREATE_SOURCE + CREATE_SINK
        //       so the sink/source identifiers exist, then
        //       tEnv.loadPlan(PlanReference.fromFile(planFile)).execute().await();
        // Note: temporary tables are NOT persisted into the plan — you must
        // re-register them (or use catalog tables that survive across runs).
        throw new UnsupportedOperationException("Implement Stage 2 — executeFromFile");
    }

    // ----- Stage 3 -----
    static void compileFiltered(Path planFile) throws IOException {
        // TODO: same as Stage 1 but compile INSERT_FILTERED instead.
        throw new UnsupportedOperationException("Implement Stage 3 — compileFiltered");
    }

    static StreamTableEnvironment newTEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }
}
