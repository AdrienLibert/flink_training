package com.training.flink.exercises;

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
import java.util.Collections;
import java.util.List;

/**
 * Lesson 8 — Mastering Connectors.
 *
 * Stage 1: a batched file sink with checkpointable buffer and at-least-once
 *          flush semantics.
 * Stage 2: wire it into a pipeline with `CountingSource` and run.
 * Stage 3 (theory): how Source V2 / Sink V2 differ from these legacy APIs,
 *                   and what exactly-once requires.
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.CustomConnectors"
 */
public class CustomConnectors {

    public static void main(String[] args) throws Exception {
        Path outFile = Files.createTempFile("orders-out-", ".csv");
        outFile.toFile().deleteOnExit();
        Files.deleteIfExists(outFile); // start clean

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500); // exercises the snapshot/restore code paths

        // Stage 2: build the pipeline.
        env.addSource(new CountingSource(20))
                .returns(Order.class)
                .addSink(new BatchedFileSink(outFile, 5));

        env.execute("Lesson 8 — Custom Connectors");

        System.out.println("[result] wrote " + Files.size(outFile) + " bytes to " + outFile);
        Files.readAllLines(outFile).forEach(line -> System.out.println("  " + line));
    }

    // ----- Stage 1 -----
    /**
     * A SinkFunction that buffers `batchSize` records in operator state and
     * flushes to disk on every checkpoint.
     *
     * TODO: implement {@link CheckpointedFunction#snapshotState} so the buffer
     * is persisted before the checkpoint barrier; otherwise a JVM crash
     * between flushes loses records.
     */
    public static class BatchedFileSink implements SinkFunction<Order>, CheckpointedFunction {

        private final String outFilePath;
        private final int batchSize;
        private transient Path outFile;
        private transient List<Order> buffer;
        private transient ListState<Order> bufferState;

        public BatchedFileSink(Path outFile, int batchSize) {
            // Store as String — java.nio.file.Path holds a non-serializable
            // FileSystem reference, and operator instances are serialized to
            // the TaskManagers.
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
            // TODO: flush() here, then update bufferState from buffer.
            // The flush makes this an at-least-once sink — a crash AFTER
            // flush but BEFORE the checkpoint completes would replay the same
            // batch on restore (duplicates). Exactly-once would require
            // staging to a temp file and renaming on notifyCheckpointComplete.
            throw new UnsupportedOperationException("Implement snapshotState");
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
