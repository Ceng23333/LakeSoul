// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.data.RowData;

import java.util.Map;
import java.util.function.Supplier;

public class LakeSoulSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
        RowData, RowData, LakeSoulPartitionSplit, LakeSoulPartitionSplit> {

    public LakeSoulSourceReader(Supplier<SplitReader<RowData, LakeSoulPartitionSplit>> splitReaderSupplier,
                                RecordEmitter<RowData, RowData, LakeSoulPartitionSplit> recordEmitter,
                                Configuration config,
                                SourceReaderContext context) {
        super(splitReaderSupplier, recordEmitter, config, context);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, LakeSoulPartitionSplit> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected LakeSoulPartitionSplit initializedState(LakeSoulPartitionSplit split) {
        return split;
    }

    @Override
    protected LakeSoulPartitionSplit toSplitType(String splitId, LakeSoulPartitionSplit splitState) {

        return splitState;
    }

}
