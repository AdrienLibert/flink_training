package com.training.flink.exercises;

import com.training.flink.model.Event;
import com.training.flink.util.PacedEventSource;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Lesson 9 — Workshop: Building Custom Operators.
 *
 * The DataStream operators you usually use (map, filter, process) are
 * thin wrappers over {@link AbstractStreamOperator}. Sometimes you need
 * the raw thing — a per-element callback PLUS direct access to the
 * processing-time service, the output collector, and the
 * RuntimeContext, with no UDF abstraction in the way. This is what the
 * Flink-internal operators look like.
 *
 * Stage 1: implement {@link ThroughputMeterOperator#processElement} so
 *          each input element passes through, incrementing a counter.
 * Stage 2: implement {@link ThroughputMeterOperator#open} to register a
 *          processing-time timer that fires every {@code reportEveryMs}
 *          ms and prints the throughput.
 * Stage 3: implement {@link ThroughputMeterOperator#onProcessingTime}
 *          to print and reset the counter, then re-register the timer.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.CustomOperatorLab"
 */
public class CustomOperatorLab {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new PacedEventSource())
                .returns(Event.class)
                .transform(
                        "ThroughputMeter",
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(Event.class),
                        new ThroughputMeterOperator(200))
                .print("OUT");

        env.execute("Lesson 9 — Custom Operator");
    }

    public static class ThroughputMeterOperator
            extends AbstractStreamOperator<Event>
            implements OneInputStreamOperator<Event, Event>, ProcessingTimeCallback {

        private final long reportEveryMs;
        private transient long counter;

        public ThroughputMeterOperator(long reportEveryMs) {
            this.reportEveryMs = reportEveryMs;
            this.chainingStrategy =
                    org.apache.flink.streaming.api.operators.ChainingStrategy.ALWAYS;
        }

        // ----- Stage 1 -----
        @Override
        public void processElement(StreamRecord<Event> element) throws Exception {
            // TODO: increment counter, then forward the record via
            //       output.collect(element).
            throw new UnsupportedOperationException("Implement processElement");
        }

        // ----- Stage 2 -----
        @Override
        public void open() throws Exception {
            super.open();
            counter = 0;
            // TODO: register a processing-time timer for now + reportEveryMs.
            // Hint: getProcessingTimeService().registerTimer(time, this).
            throw new UnsupportedOperationException("Implement open");
        }

        // ----- Stage 3 -----
        @Override
        public void onProcessingTime(long timestamp) throws Exception {
            // TODO: print "[meter] elements=" + counter for the past tick,
            //       reset counter to 0, and re-register the next timer at
            //       getProcessingTimeService().getCurrentProcessingTime() + reportEveryMs.
            throw new UnsupportedOperationException("Implement onProcessingTime");
        }
    }
}
