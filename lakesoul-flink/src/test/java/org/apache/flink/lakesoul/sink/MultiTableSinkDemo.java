package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.NativeOptions;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BATCH_SIZE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;

public class MultiTableSinkDemo {

    static long checkpointInterval = 5 * 1000;
    static int tableNum = 8;

    public static void main(String[] args) throws Exception {
        new LakeSoulCatalog().cleanForTest();

        Configuration conf = new Configuration();
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("512m"));
        conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("512m"));
        conf.set(NativeOptions.MEM_LIMIT, String.valueOf(1024 * 1024 * 10));
//        conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.parse("20m"));
//        conf.set(TaskManagerOptions.JVM_METASPACE, MemorySize.parse("512m"));
//        conf.set(ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER, 2);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);

        int sourceParallelism = 8;


        DataStreamSource<BinarySourceRecord>
                source =
                env.addSource(new BinarySourceRecordDataGenSource())
                        .setParallelism(sourceParallelism);

        env.getCheckpointConfig().setCheckpointInterval(checkpointInterval);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 1000L));
        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = (Configuration) env.getConfiguration();

        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf, conf.getString(SERVER_TIME_ZONE));
        LakeSoulMultiTableSinkStreamBuilder
                builder =
                new LakeSoulMultiTableSinkStreamBuilder((Source) source, context, lakeSoulRecordConvert);
        builder.buildLakeSoulDMLSink(source);

//        context.conf.set(MAX_ROW_GROUP_VALUE_NUMBER, rowGroupValues);

//        String name = "Print Sink";
//        PrintSinkFunction<LakeSoulArrowWrapper> printFunction = new PrintSinkFunction<>(name, false);
//
//        DataStreamSink<LakeSoulArrowWrapper> sink = source.addSink(printFunction).name(name).setParallelism(2);

        env.execute("Test MultiTable Sink");
    }

    public static class BinarySourceRecordDataGenSource extends RichParallelSourceFunction<BinarySourceRecord>
            implements CheckpointedFunction {
        private static final Logger LOG = LoggerFactory.getLogger(BinarySourceRecordDataGenSource.class);

        private transient ListState<Integer> checkpointedCount;
        public int count;

        private volatile transient boolean isRunning;

        /**
         * This method is called when a snapshot for a checkpoint is requested. This acts as a hook to
         * the function to ensure that all state is exposed by means previously offered through {@link
         * FunctionInitializationContext} when the Function was initialized, or offered now by {@link
         * FunctionSnapshotContext} itself.
         *
         * @param context the context for drawing a snapshot of the operator
         * @throws Exception Thrown, if state could not be created ot restored.
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("============= Source snapshotState getCheckpointId=" + context.getCheckpointId() + " ================");
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + " snapshotState context.getCheckpointId=" + context.getCheckpointId() + ", count=" + count);
            this.checkpointedCount.clear();
            try {
                this.checkpointedCount.add(count);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.info("Snapshot state, generated {} batches", count);
        }

        /**
         * This method is called when the parallel function instance is created during distributed
         * execution. Functions typically set up their state storing data structures in this method.
         *
         * @param context the context for initializing the operator
         * @throws Exception Thrown, if state could not be created ot restored.
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            isRunning = true;
            try {
                this.checkpointedCount = context
                        .getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("count", Integer.class));

                if (context.isRestored()) {
                    for (Integer count : this.checkpointedCount.get()) {
                        this.count = count;
                    }
                }
                System.out.println("initializeState count=" + count);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Starts the source. Implementations use the {@link SourceContext} to emit elements. Sources
         * that checkpoint their state for fault tolerance should use the {@link
         * SourceContext#getCheckpointLock()} checkpoint lock} to ensure consistency between the
         * bookkeeping and emitting the elements.
         *
         * <p>Sources that implement {@link CheckpointedFunction} must lock on the {@link
         * SourceContext#getCheckpointLock()} checkpoint lock} checkpoint lock (using a synchronized
         * block) before updating internal state and emitting elements, to make both an atomic
         * operation.
         *
         * <p>Refer to the {@link SourceFunction top-level class docs} for an example.
         *
         * @param ctx The context to emit elements to and for accessing locks.
         */
        @Override
        public void run(SourceContext<BinarySourceRecord> ctx) throws Exception {

        }

        /**
         * Cancels the source. Most sources will have a while loop inside the {@link
         * #run(SourceContext)} method. The implementation needs to ensure that the source will break
         * out of that loop after this method is called.
         *
         * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
         * {@code false} in this method. That flag is checked in the loop condition.
         *
         * <p>In case of an ungraceful shutdown (cancellation of the source operator, possibly for
         * failover), the thread that calls {@link #run(SourceContext)} will also be {@link
         * Thread#interrupt() interrupted}) by the Flink runtime, in order to speed up the cancellation
         * (to ensure threads exit blocking methods fast, like I/O, blocking queues, etc.). The
         * interruption happens strictly after this method has been called, so any interruption handler
         * can rely on the fact that this method has completed (for example to ignore exceptions that
         * happen after cancellation).
         *
         * <p>During graceful shutdown (for example stopping a job with a savepoint), the program must
         * cleanly exit the {@link #run(SourceContext)} method soon after this method was called. The
         * Flink runtime will NOT interrupt the source thread during graceful shutdown. Source
         * implementors must ensure that no thread interruption happens on any thread that emits records
         * through the {@code SourceContext} from the {@link #run(SourceContext)} method; otherwise the
         * clean shutdown may fail when threads are interrupted while processing the final records.
         *
         * <p>Because the {@code SourceFunction} cannot easily differentiate whether the shutdown should
         * be graceful or ungraceful, we recommend that implementors refrain from interrupting any
         * threads that interact with the {@code SourceContext} at all. You can rely on the Flink
         * runtime to interrupt the source thread in case of ungraceful cancellation. Any additionally
         * spawned threads that directly emit records through the {@code SourceContext} should use a
         * shutdown method that does not rely on thread interruption.
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
