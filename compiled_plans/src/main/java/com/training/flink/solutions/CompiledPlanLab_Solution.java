package com.training.flink.solutions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class CompiledPlanLab_Solution {

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
        Files.deleteIfExists(planFile);
        planFile.toFile().deleteOnExit();

        compileBaseline(planFile);
        System.out.println("[Stage 1] wrote plan: " + planFile
                + " (" + Files.size(planFile) + " bytes)");

        executeFromFile(planFile);

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

    static void compileBaseline(Path planFile) throws IOException {
        StreamTableEnvironment tEnv = newTEnv();
        tEnv.executeSql(CREATE_SOURCE);
        tEnv.executeSql(CREATE_SINK);
        CompiledPlan plan = tEnv.compilePlanSql(INSERT_BASELINE);
        plan.writeToFile(planFile.toFile(), false);
    }

    static void executeFromFile(Path planFile) throws Exception {
        StreamTableEnvironment tEnv = newTEnv();
        tEnv.executeSql(CREATE_SOURCE);
        tEnv.executeSql(CREATE_SINK);
        tEnv.loadPlan(PlanReference.fromFile(planFile.toFile())).execute().await();
    }

    static void compileFiltered(Path planFile) throws IOException {
        StreamTableEnvironment tEnv = newTEnv();
        tEnv.executeSql(CREATE_SOURCE);
        tEnv.executeSql(CREATE_SINK);
        CompiledPlan plan = tEnv.compilePlanSql(INSERT_FILTERED);
        plan.writeToFile(planFile.toFile(), false);
    }

    static StreamTableEnvironment newTEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }
}
