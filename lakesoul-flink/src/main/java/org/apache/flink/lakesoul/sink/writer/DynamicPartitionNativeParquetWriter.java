// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SORT_FIELD;

public class DynamicPartitionNativeParquetWriter implements InProgressFileWriter<RowData, String> {

    private final ArrowWriter<RowData> arrowWriter;

    private NativeIOWriter nativeWriter;

    private final int batchSize;

    private final long creationTime;

    private final VectorSchemaRoot batch;

    private int rowsInBatch;

    long lastUpdateTime;

    String prefix;

    private long totalRows = 0;

    public DynamicPartitionNativeParquetWriter(RowType rowType,
                                               List<String> primaryKeys,
                                               List<String> rangeColumns,
                                               Path path,
                                               long creationTime,
                                               Configuration conf) throws IOException {
        this.batchSize = 250000; // keep same with native writer's row group row number
        this.creationTime = creationTime;
        this.rowsInBatch = 0;

        ArrowUtils.setLocalTimeZone(FlinkUtil.getLocalTimeZone(conf));
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        nativeWriter = new NativeIOWriter(arrowSchema);
        nativeWriter.setPrimaryKeys(primaryKeys);
        nativeWriter.setRangePartitions(rangeColumns);
        if (conf.getBoolean(LakeSoulSinkOptions.isMultiTableSource)) {
            nativeWriter.setAuxSortColumns(Collections.singletonList(SORT_FIELD));
        }
        nativeWriter.setRowGroupRowNumber(this.batchSize);
        batch = VectorSchemaRoot.create(arrowSchema, nativeWriter.getAllocator());
        arrowWriter = ArrowUtils.createRowDataArrowWriter(batch, rowType);
        this.prefix = path.makeQualified(path.getFileSystem()).toString();

        nativeWriter.withPrefix(this.prefix);
        nativeWriter.useDynamicPartition(true);

        FlinkUtil.setFSConfigs(conf, this.nativeWriter);
        this.nativeWriter.initializeWriter();
    }

    @Override
    public void write(RowData element, long currentTime) throws IOException {
        this.lastUpdateTime = currentTime;
        this.arrowWriter.write(element);
        this.rowsInBatch++;
        this.totalRows++;
        if (this.rowsInBatch >= this.batchSize) {
            this.arrowWriter.finish();
            this.nativeWriter.write(this.batch);
            // in native writer, batch may be kept in memory for sorting,
            // so we have to release ownership in java
            this.batch.clear();
            this.arrowWriter.reset();
            this.rowsInBatch = 0;
        }
    }

    @Override
    public InProgressFileRecoverable persist() throws IOException {
        // we currently do not support persist
        return null;
    }

    static public class PartitionDescAndPendingFilesRecoverable implements PendingFileRecoverable, Serializable {
        public String path;

        public long creationTime;

        PartitionDescAndPendingFilesRecoverable(String path, long creationTime) {
            this.path = path;
            this.creationTime = creationTime;
        }

        @Override
        public String toString() {
            return "PendingFile(" +
                    path + ", " + creationTime + ")";
        }

        @Nullable
        @Override
        public Path getPath() {
            return new Path(path);
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, creationTime);
        }
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        this.arrowWriter.finish();
        this.nativeWriter.write(this.batch);
        HashMap<String, List<String>> partitionDescAndFilesMap = this.nativeWriter.flush();
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
        this.batch.clear();
        this.batch.close();
        try {
            this.nativeWriter.close();
            this.nativeWriter = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new PartitionDescAndPendingFilesRecoverable(this.prefix, this.creationTime);
    }

    @Override
    public void dispose() {
        try {
            this.arrowWriter.finish();
            this.batch.close();
            this.nativeWriter.close();
            this.nativeWriter = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getBucketId() {
        return "DynamicBucket";
    }

    @Override
    public long getCreationTime() {
        return this.creationTime;
    }

    @Override
    public long getSize() throws IOException {
        return totalRows;
    }

    @Override
    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
}
