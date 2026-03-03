package org.apache.flink.connector.jdbc.plus.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplitState;
import org.apache.flink.table.data.RowData;

/**
 * Emits {@link RowData} records from the fetch-thread queue to the Flink output and advances the
 * split offset for checkpoint tracking.
 *
 * <p>In the FLIP-27 {@code SourceReaderBase} model:
 *
 * <ol>
 *   <li>The {@link JdbcSplitReader} runs in a background (producer) thread and places {@link
 *       JdbcResultSetRecords} batches into a {@code FutureCompletingBlockingQueue}.
 *   <li>The main reader thread in {@link JdbcSourceReader} calls {@code
 *       SourceReaderBase.pollNext()}, which dequeues a batch and calls {@link #emitRecord} for each
 *       element.
 *   <li>This emitter forwards the record downstream and increments the per-split {@link
 *       JdbcSourceSplitState#incrementOffset() offset} so that, on a checkpoint and subsequent
 *       restart, the reader can resume without re-emitting already-sent rows.
 * </ol>
 */
public class JdbcRecordEmitter implements RecordEmitter<RowData, RowData, JdbcSourceSplitState> {

    @Override
    public void emitRecord(
            RowData element, SourceOutput<RowData> output, JdbcSourceSplitState splitState) {
        output.collect(element);
        splitState.incrementOffset();
    }
}
