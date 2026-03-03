package org.apache.flink.connector.jdbc.plus.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Adapts a JDBC {@link ResultSet} to the {@link RecordsWithSplitIds} interface required by Flink's
 * {@code SourceReaderBase}.
 *
 * <h2>Lifecycle contract</h2>
 *
 * <ol>
 *   <li>{@link #nextSplit()} is called once; it returns the split ID on the first call and {@code
 *       null} thereafter.
 *   <li>{@link #nextRecordFromSplit()} is called repeatedly until it returns {@code null}, at which
 *       point the ResultSet is exhausted.
 *   <li>{@link #finishedSplits()} is queried after iteration is complete; it returns a singleton
 *       containing this split's ID.
 *   <li>{@link #recycle()} closes the underlying ResultSet and Statement.
 * </ol>
 *
 * <p>This class is deliberately single-use (one split per instance) which keeps the implementation
 * simple and avoids state management across multiple ResultSets.
 */
class JdbcResultSetRecords implements RecordsWithSplitIds<RowData> {

    private final String splitId;
    private final ResultSet resultSet;
    private final JdbcRowConverter rowConverter;
    private final Runnable onClose;

    /** {@code true} after {@link #nextSplit()} has been called once. */
    private boolean splitConsumed = false;

    /** {@code true} after the ResultSet has returned all rows. */
    private boolean exhausted = false;

    JdbcResultSetRecords(
            String splitId, ResultSet resultSet, JdbcRowConverter rowConverter, Runnable onClose) {
        this.splitId = splitId;
        this.resultSet = resultSet;
        this.rowConverter = rowConverter;
        this.onClose = onClose;
    }

    // -------------------------------------------------------------------------
    // RecordsWithSplitIds
    // -------------------------------------------------------------------------

    /**
     * Advances to the next split in this batch. Since each instance represents exactly one split,
     * this returns the split ID on the first call and {@code null} thereafter.
     */
    @Nullable
    @Override
    public String nextSplit() {
        if (!splitConsumed) {
            splitConsumed = true;
            return splitId;
        }
        return null;
    }

    /**
     * Returns the next {@link RowData} record, or {@code null} when the ResultSet is exhausted
     * (signalling the end of this split's data to {@code SourceReaderBase}).
     */
    @Nullable
    @Override
    public RowData nextRecordFromSplit() {
        if (exhausted) {
            return null;
        }
        try {
            if (resultSet.next()) {
                return rowConverter.toInternal(resultSet);
            } else {
                exhausted = true;
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Error reading record from JDBC ResultSet for split " + splitId, e);
        }
    }

    /**
     * Reports which splits are "done" from the fetcher's perspective.
     *
     * <p>This method is called by Flink's {@code FetchTask} in two situations:
     *
     * <ol>
     *   <li><b>Immediately after the batch is put into the elements queue</b> (FetchTask side):
     *       Flink uses the returned IDs to remove splits from the {@code SplitFetcher}'s internal
     *       {@code assignedSplits} map. Only when {@code assignedSplits} is empty does {@code
     *       SplitFetcher.isIdle()} return {@code true}, which in turn lets {@code
     *       SplitFetcherManager.maybeShutdownFinishedFetchers()} return {@code true}, which is a
     *       required condition for {@code SourceReaderBase.finishedOrAvailable()} to emit {@code
     *       END_OF_INPUT}.
     *   <li><b>When the main reader thread finishes iterating the batch</b> (SourceReaderBase side,
     *       inside {@code finishCurrentFetch}): Flink removes these IDs from {@code splitStates}
     *       and calls {@code onSplitFinished}.
     * </ol>
     *
     * <p>We must return {@code {splitId}} unconditionally (not gated on {@code exhausted}) because
     * at call-site (1) the main thread has not yet consumed any records — so {@code exhausted} is
     * always {@code false} there — yet the split <em>will</em> complete after the main thread
     * iterates to the end. Gating on {@code exhausted} causes the split to remain in {@code
     * assignedSplits} forever, making the fetcher appear permanently busy and preventing the job
     * from terminating.
     */
    @Override
    public Set<String> finishedSplits() {
        return splitId != null ? Collections.singleton(splitId) : Collections.emptySet();
    }

    /** Closes the underlying ResultSet and triggers any registered {@code onClose} callback. */
    @Override
    public void recycle() {
        try {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }
        } catch (SQLException e) {
            // best-effort
        } finally {
            onClose.run();
        }
    }

    // ── Static factory for an empty (no-data) batch ──────────────────────────

    /** Returns an empty {@link RecordsWithSplitIds} with no records and no finished splits. */
    static RecordsWithSplitIds<RowData> empty() {
        return EmptyRecords.INSTANCE;
    }

    private static final class EmptyRecords implements RecordsWithSplitIds<RowData> {
        static final EmptyRecords INSTANCE = new EmptyRecords();

        @Nullable
        @Override
        public String nextSplit() {
            return null;
        }

        @Nullable
        @Override
        public RowData nextRecordFromSplit() {
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }

        @Override
        public void recycle() {}
    }
}
