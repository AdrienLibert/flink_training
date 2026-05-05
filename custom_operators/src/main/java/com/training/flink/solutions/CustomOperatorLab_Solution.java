package com.training.flink.solutions;

import com.training.flink.model.Event;
import com.training.flink.util.PacedEventSource;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class CustomOperatorLab_Solution {

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

        env.execute("Lesson 9 — Custom Operator (solution)");
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

        @Override
        public void processElement(StreamRecord<Event> element) throws Exception {
            counter++;
            output.collect(element);
        }

        @Override
        public void open() throws Exception {
            super.open();
            counter = 0;
            long now = getProcessingTimeService().getCurrentProcessingTime();
            getProcessingTimeService().registerTimer(now + reportEveryMs, this);
        }

        @Override
        public void onProcessingTime(long timestamp) throws Exception {
            System.out.println("[meter] elements in last " + reportEveryMs + "ms = " + counter);
            counter = 0;
            long next = getProcessingTimeService().getCurrentProcessingTime() + reportEveryMs;
            getProcessingTimeService().registerTimer(next, this);
        }
    }
}
