package org.apache.flink.connector.jdbc.plus.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.jdbc.plus.JdbcPlusOptions;
import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplitState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

/**
 * TaskManager-side {@code SourceReader} for the Flink JDBC Plus connector.
 *
 * <h2>Architecture</h2>
 *
 * <p>Extends {@link SingleThreadMultiplexSourceReaderBase}, which wires together:
 *
 * <ul>
 *   <li>A {@code SingleThreadFetcherManager} that runs {@link JdbcSplitReader} in a dedicated
 *       background (producer) thread. Fetched {@link JdbcResultSetRecords} batches are placed into
 *       a {@code FutureCompletingBlockingQueue}.
 *   <li>The main Flink-runtime thread that calls {@code pollNext()}, dequeues batches, and
 *       delegates to {@link JdbcRecordEmitter} for downstream forwarding.
 * </ul>
 *
 * <p>This explicit producer/consumer separation ensures that:
 *
 * <ul>
 *   <li>JDBC I/O never blocks the Flink main thread.
 *   <li>The reader can be paused (back-pressure) without dropping JDBC cursors.
 *   <li>JM and TM communicate only via the FLIP-27 split-assignment messages — there is no shared
 *       state between the enumerator ({@code JdbcSourceEnumerator}) and this reader.
 * </ul>
 *
 * <h2>Checkpointing</h2>
 *
 * <p>{@link #initializedState} and {@link #toSplitType} bridge between the mutable {@link
 * JdbcSourceSplitState} (incremented per record) and the immutable {@link JdbcSourceSplit} snapshot
 * stored in Flink checkpoints.
 */
public class JdbcSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                RowData, RowData, JdbcSourceSplit, JdbcSourceSplitState> {

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {
        super.addSplits(splits);
    }

    @Override
    public void start() {
        super.context.sendSplitRequest();
    }

    public JdbcSourceReader(
            SourceReaderContext context,
            JdbcPlusOptions options,
            JdbcDialect dialect,
            RowType rowType) {
        super(
                () -> new JdbcSplitReader(options, dialect, rowType),
                new JdbcRecordEmitter(),
                toConfiguration(options),
                context);
    }

    // -------------------------------------------------------------------------
    // Split state management
    // -------------------------------------------------------------------------

    /** Creates a fresh mutable state for a newly assigned split (zero offset). */
    @Override
    protected JdbcSourceSplitState initializedState(JdbcSourceSplit split) {
        return new JdbcSourceSplitState(split);
    }

    /** Converts the mutable state back to an immutable split for checkpoint serialization. */
    @Override
    protected JdbcSourceSplit toSplitType(String splitId, JdbcSourceSplitState splitState) {
        return splitState.toSourceSplit();
    }

    /**
     * Called by the framework after a split is fully consumed.
     *
     * <p>We MUST call {@link
     * org.apache.flink.api.connector.source.SourceReaderContext#sendSplitRequest()} here; otherwise
     * the enumerator never assigns the next split (and never sends "no more splits"), so the job
     * hangs in RUNNING state forever without emitting any output in batch mode.
     *
     * <p>The enumerator will respond either with the next pending split or with a "no more splits"
     * signal, which lets {@code SourceReaderBase} transition to {@link
     * org.apache.flink.api.connector.source.ReaderOutput} finished state.
     */
    @Override
    protected void onSplitFinished(Map<String, JdbcSourceSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    // -------------------------------------------------------------------------
    // Configuration helpers
    // -------------------------------------------------------------------------

    private static Configuration toConfiguration(JdbcPlusOptions options) {
        Configuration config = new Configuration();
        // The element queue capacity can be tuned here if needed.
        // For now we rely on Flink defaults.
        return config;
    }
}
