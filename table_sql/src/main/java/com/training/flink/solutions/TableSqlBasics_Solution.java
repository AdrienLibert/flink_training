package com.training.flink.solutions;

import com.training.flink.model.Order;
import com.training.flink.util.OrderSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableSqlBasics_Solution {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Order> raw = env.addSource(new OrderSource()).returns(Order.class);

        Table orders = ordersTable(tEnv, raw);
        tEnv.createTemporaryView("orders", orders);

        tEnv.toChangelogStream(totalsByCategory(tEnv)).print("CAT-TOTAL");
        tEnv.toDataStream(windowedByCategory(tEnv)).print("WINDOW-CAT");
        tEnv.toChangelogStream(topSpenderPerCategory(tEnv)).print("TOP-SPENDER");

        env.execute("Lesson 5 — Table & SQL Basics (solution)");
    }

    static Table ordersTable(StreamTableEnvironment tEnv, DataStream<Order> raw) {
        Schema schema = Schema.newBuilder()
                .column("orderId", "STRING")
                .column("userId", "STRING")
                .column("category", "STRING")
                .column("amount", "DOUBLE")
                .column("ts", "BIGINT")
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("event_time", "event_time - INTERVAL '2' SECOND")
                .build();
        return tEnv.fromDataStream(raw, schema);
    }

    static Table totalsByCategory(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "SELECT category, SUM(amount) AS revenue "
                        + "FROM orders GROUP BY category");
    }

    static Table windowedByCategory(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "SELECT window_end, category, SUM(amount) AS revenue "
                        + "FROM TABLE(TUMBLE("
                        + "  TABLE orders, DESCRIPTOR(event_time), INTERVAL '5' SECOND)) "
                        + "GROUP BY window_start, window_end, category");
    }

    static Table topSpenderPerCategory(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "SELECT category, userId, total FROM ("
                        + "  SELECT category, userId, SUM(amount) AS total,"
                        + "         ROW_NUMBER() OVER ("
                        + "             PARTITION BY category ORDER BY SUM(amount) DESC) AS rn"
                        + "  FROM orders GROUP BY category, userId"
                        + ") WHERE rn = 1");
    }
}
