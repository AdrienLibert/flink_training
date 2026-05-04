package com.training.flink.exercises;

import com.training.flink.model.ClickEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Exercise 4: Side Outputs & Routing
 *
 * Goal:
 *   Split the input stream into THREE separate outputs:
 *
 *   1. MAIN output:           high-value purchases (price > 500)
 *   2. Side output "regular": purchases with price <= 500
 *   3. Side output "non-purchase": every event whose action is NOT "purchase"
 *
 *   Print each output with a clear label so you can see all three streams in the console.
 *
 * Requirements:
 *   - Use a single ProcessFunction<ClickEvent, ClickEvent> (NOT a KeyedProcessFunction).
 *   - Use OutputTag<ClickEvent> to declare side-output channels.
 *   - Inside processElement, emit to main via out.collect(...), and to side outputs via ctx.output(tag, ...).
 *   - getSideOutput(tag) on the resulting stream gives you the side stream.
 *
 * Run:
 *   mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise4_SideOutputs"
 *
 * Why side outputs (vs filter+filter+filter)?
 *   - One pass over the data: each input is dispatched once, not three times.
 *   - Different sinks/serializers per output (cheap routing without a full split).
 *   - Common production pattern: main output is happy path, side outputs are
 *     "DLQ" / "late events" / "validation failures" / "metrics".
 *
 * Bonus question (answer in a comment):
 *   What's the difference between OutputTag<T> and an OutputTag declared with a TypeHint
 *   like `new OutputTag<ClickEvent>("name") {}` (note the trailing {})? When does Flink
 *   require the anonymous-subclass form?
 */
public class Exercise4_SideOutputs {

    // TODO 1: declare two OutputTag<ClickEvent> static finals, one for "regular" and one for "non-purchase".
    //   Hint: Flink needs you to use the anonymous-subclass form `new OutputTag<ClickEvent>("name") {}`
    //   so it can extract the generic type at runtime.

    private static final OutputTag<ClickEvent> REGULAR_TAG = 
        new OutputTag<ClickEvent>("regular-events") {};

    private static final OutputTag<ClickEvent> NON_PURCHASE_TAG = 
        new OutputTag<ClickEvent>("non-purchase-events") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ClickEvent> clicks = env.fromElements(
                new ClickEvent("u1", "p100", "electronics",   299.99, 1000L, "view"),
                new ClickEvent("u2", "p200", "books",          19.50, 2000L, "purchase"),         // regular
                new ClickEvent("u3", "p300", "electronics",   799.00, 3000L, "purchase"),         // HIGH-VALUE
                new ClickEvent("u4", "p400", "clothing",      120.00, 4000L, "add_to_cart"),
                new ClickEvent("u5", "p500", "electronics",  1299.00, 5000L, "purchase"),         // HIGH-VALUE
                new ClickEvent("u6", "p600", "books",          25.00, 6000L, "purchase"),         // regular
                new ClickEvent("u7", "p700", "clothing",       89.99, 7000L, "view")
        );

        SingleOutputStreamOperator<ClickEvent> mainStream = clicks.process(
            new ProcessFunction<ClickEvent, ClickEvent>() {
                @Override
                public void processElement(ClickEvent event, Context ctx, Collector<ClickEvent> out) throws Exception {
                    
                    if ("purchase".equals(event.getAction())) {
                        if (event.getPrice() > 500) {
                            out.collect(event);
                        } else {
                            ctx.output(REGULAR_TAG, event);
                        }
                    } else {
                        ctx.output(NON_PURCHASE_TAG, event);
                    }
                }
            }
        );

        DataStream<ClickEvent> expensivePurchases = mainStream; // The main result
        DataStream<ClickEvent> regularPurchases = mainStream.getSideOutput(REGULAR_TAG);
        DataStream<ClickEvent> otherEvents = mainStream.getSideOutput(NON_PURCHASE_TAG);
        expensivePurchases.print("HIGH-VALUE");
        regularPurchases.print("REGULAR");
        otherEvents.print("NON-PURCHASE");

        env.execute("Exercise 4 - Side Outputs");
    }
}
