package com.training.flink.solutions;

import com.training.flink.model.ClickEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Exercise 4 — Solution: Side Outputs & Routing
 *
 * Single-pass routing into 3 streams:
 *   - main:                    high-value purchases (price > 500)
 *   - side "regular":          purchases with price <= 500
 *   - side "non-purchase":     everything else
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.solutions.Exercise4_SideOutputs_Solution"
 *
 * --------------------------------------------------------------------------
 * BONUS QUESTION:
 *   What's the difference between OutputTag<T> and an OutputTag declared with
 *   a TypeHint like `new OutputTag<ClickEvent>("name") {}` (note the {}).
 *   When does Flink require the anonymous-subclass form?
 *
 * ANSWER:
 *   `new OutputTag<ClickEvent>("name")` doesn't compile-time-error, but
 *   `<ClickEvent>` is erased at runtime — Flink can't recover the type.
 *   Side outputs need a TypeInformation to choose a serializer, so this form
 *   throws InvalidTypesException when the job is submitted.
 *
 *   `new OutputTag<ClickEvent>("name") {}` (note the trailing braces) creates
 *   an ANONYMOUS SUBCLASS of OutputTag. Anonymous subclasses preserve their
 *   parameterized supertype in the bytecode's `Signature` attribute, which
 *   Flink reads via reflection to recover the full generic type at runtime.
 *
 *   Rule of thumb: ALWAYS use the `{}` form for OutputTag in Flink.
 *
 *   This is the same trick Guava's TypeToken and Jackson's TypeReference use,
 *   and it's the same family of problems we hit with Tuple2 lambdas — just
 *   solved differently. (For lambdas the fix is .returns(...); for OutputTag
 *   the fix is the anonymous subclass.)
 *
 *   Why side outputs vs. multi-filter pipelines?
 *     - One pass over the data, not three.
 *     - Different output types per branch are allowed (you could have
 *       OutputTag<Alert> alongside main DataStream<ClickEvent>).
 *     - Common production pattern: main = happy path, side outputs = DLQ /
 *       late events / validation failures / ad-hoc metrics.
 * --------------------------------------------------------------------------
 */
public class Exercise4_SideOutputs_Solution {

    private static final OutputTag<ClickEvent> REGULAR_TAG =
            new OutputTag<ClickEvent>("regular") {};

    private static final OutputTag<ClickEvent> NON_PURCHASE_TAG =
            new OutputTag<ClickEvent>("non-purchase") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "p100", "electronics",   299.99, 1000L, "view"),
                new ClickEvent("u2", "p200", "books",          19.50, 2000L, "purchase"),
                new ClickEvent("u3", "p300", "electronics",   799.00, 3000L, "purchase"),
                new ClickEvent("u4", "p400", "clothing",      120.00, 4000L, "add_to_cart"),
                new ClickEvent("u5", "p500", "electronics",  1299.00, 5000L, "purchase"),
                new ClickEvent("u6", "p600", "books",          25.00, 6000L, "purchase"),
                new ClickEvent("u7", "p700", "clothing",       89.99, 7000L, "view")
        );

        SingleOutputStreamOperator<ClickEvent> highValue = clicks.process(
                new ProcessFunction<ClickEvent, ClickEvent>() {
                    @Override
                    public void processElement(ClickEvent event,
                                               Context ctx,
                                               Collector<ClickEvent> out) {
                        if ("purchase".equals(event.action)) {
                            if (event.price > 500.0) {
                                out.collect(event);                     // main output
                            } else {
                                ctx.output(REGULAR_TAG, event);         // side: regular
                            }
                        } else {
                            ctx.output(NON_PURCHASE_TAG, event);        // side: non-purchase
                        }
                    }
                }
        );

        DataStream<ClickEvent> regular     = highValue.getSideOutput(REGULAR_TAG);
        DataStream<ClickEvent> nonPurchase = highValue.getSideOutput(NON_PURCHASE_TAG);

        highValue.print("HIGH-VALUE");
        regular.print("REGULAR");
        nonPurchase.print("NON-PURCHASE");

        env.execute("Exercise 4 - Side Outputs (solution)");
    }
}
