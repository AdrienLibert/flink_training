package com.training.flink.solutions;

import com.training.flink.model.Order;
import com.training.flink.util.CountingSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class CustomConnectors_Solution {

    public static void main(String[] args) throws Exception {
        Path outFile = Files.createTempFile("orders-out-", ".csv");
        outFile.toFile().deleteOnExit();
        Files.deleteIfExists(outFile);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);

        env.addSource(new CountingSource(20))
                .returns(Order.class)
                .addSink(new BatchedFileSink(outFile, 5));

        env.execute("Lesson 8 — Custom Connectors (solution)");

        System.out.println("[result] wrote " + Files.size(outFile) + " bytes to " + outFile);
        Files.readAllLines(outFile).forEach(line -> System.out.println("  " + line));
    }

    public static class BatchedFileSink implements SinkFunction<Order>, CheckpointedFunction {

        private final String outFilePath;
        private final int batchSize;
        private transient Path outFile;
        private transient List<Order> buffer;
        private transient ListState<Order> bufferState;

        public BatchedFileSink(Path outFile, int batchSize) {
            this.outFilePath = outFile.toString();
            this.batchSize = batchSize;
        }

        @Override
        public void invoke(Order value, Context context) throws Exception {
            buffer.add(value);
            if (buffer.size() >= batchSize) {
                flush();
            }
        }

        private void flush() throws IOException {
            if (buffer.isEmpty()) return;
            try (BufferedWriter w = Files.newBufferedWriter(
                    outFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                for (Order o : buffer) {
                    w.write(o.toCsv());
                    w.newLine();
                }
            }
            buffer.clear();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // At-least-once: flush whatever's buffered so committed bytes are
            // up-to-date with the checkpoint, then mirror the (now empty)
            // buffer into operator state so restore is consistent.
            flush();
            bufferState.update(new ArrayList<>(buffer));
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            outFile = java.nio.file.Paths.get(outFilePath);
            buffer = new ArrayList<>();
            bufferState = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("buffer", Types.POJO(Order.class)));
            if (ctx.isRestored()) {
                for (Order o : bufferState.get()) {
                    buffer.add(o);
                }
            }
        }
    }
}
