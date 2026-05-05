package com.training.flink.exercises;

import com.training.flink.model.Order;
import com.training.flink.util.OrderSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Lesson 5 — Mastering Table & SQL API.
 *
 * Stage 1: register a DataStream as a Table with a watermarked event-time column.
 * Stage 2: SQL aggregation — total spend per category (changelog stream).
 * Stage 3: SQL tumbling event-time window — revenue per category per 5s.
 * Stage 4 (bonus): top spender per category via ROW_NUMBER().
 *
 * Each stage method throws UnsupportedOperationException until you fill it in.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.TableSqlBasics"
 */
public class TableSqlBasics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Order> raw = env.addSource(new OrderSource()).returns(Order.class);

        // Stage 1
        Table orders = ordersTable(tEnv, raw);
        tEnv.createTemporaryView("orders", orders);

        // Stage 2 — non-windowed group by → changelog (insert/update).
        tEnv.toChangelogStream(totalsByCategory(tEnv)).print("CAT-TOTAL");

        // Stage 3 — windowed group by → append-only.
        tEnv.toDataStream(windowedByCategory(tEnv)).print("WINDOW-CAT");

        // Stage 4 — bonus. Uncomment when you've implemented it.
        // tEnv.toChangelogStream(topSpenderPerCategory(tEnv)).print("TOP-SPENDER");

        env.execute("Lesson 5 — Table & SQL Basics");
    }

    // ----- Stage 1 -----
    static Table ordersTable(StreamTableEnvironment tEnv, DataStream<Order> raw) {
        // TODO: build a Schema with:
        //   - all four scalar columns from Order
        //   - an `event_time` computed column: columnByExpression(
        //         "event_time", "TO_TIMESTAMP_LTZ(ts, 3)")
        //   - a watermark on `event_time` of `event_time - INTERVAL '2' SECOND`
        // Then return tEnv.fromDataStream(raw, schema).
        //
        // Hint: see org.apache.flink.table.api.Schema.newBuilder().
        throw new UnsupportedOperationException("Implement Stage 1 — ordersTable");
    }

    // ----- Stage 2 -----
    static Table totalsByCategory(StreamTableEnvironment tEnv) {
        // TODO: SELECT category, SUM(amount) AS revenue FROM orders GROUP BY category
        // Note: this is a non-windowed aggregation, so the result is a CHANGELOG
        // (rows are updated as new orders arrive). Use toChangelogStream(...).
        throw new UnsupportedOperationException("Implement Stage 2 — totalsByCategory");
    }

    // ----- Stage 3 -----
    static Table windowedByCategory(StreamTableEnvironment tEnv) {
        // TODO: tumbling 5s event-time window per category, using the TUMBLE
        // table-valued function:
        //   SELECT window_end, category, SUM(amount) AS revenue
        //   FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '5' SECOND))
        //   GROUP BY window_start, window_end, category
        // Result is append-only → toDataStream(...) is fine.
        throw new UnsupportedOperationException("Implement Stage 3 — windowedByCategory");
    }

    // ----- Stage 4 (Bonus) -----
    static Table topSpenderPerCategory(StreamTableEnvironment tEnv) {
        // BONUS: per-category top spender via ROW_NUMBER() — Flink's standard top-N pattern.
        //   SELECT category, userId, total
        //   FROM (
        //       SELECT category, userId, SUM(amount) AS total,
        //              ROW_NUMBER() OVER (
        //                  PARTITION BY category ORDER BY SUM(amount) DESC) AS rn
        //       FROM orders
        //       GROUP BY category, userId
        //   ) WHERE rn = 1
        throw new UnsupportedOperationException("Implement Stage 4 — topSpenderPerCategory (bonus)");
    }
}
