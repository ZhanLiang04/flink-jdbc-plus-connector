package org.apache.flink.connector.jdbc.plus.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.jdbc.plus.JdbcPlusOptions;
import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Properties;

/**
 * TaskManager-side low-level reader that executes JDBC queries for each assigned {@link
 * JdbcSourceSplit} and surfaces rows as a {@link RecordsWithSplitIds} stream.
 *
 * <h2>Responsibilities</h2>
 *
 * <ul>
 *   <li>Maintains one persistent JDBC connection per subtask (opened lazily).
 *   <li>Accepts splits from the {@link JdbcSourceEnumerator} via {@link
 *       #handleSplitsChanges(SplitsChange)} and queues them locally.
 *   <li>On each call to {@link #fetch()}, opens the next pending split's result set and wraps it in
 *       a {@link JdbcResultSetRecords}. The framework's {@code SplitFetcherManager} calls {@code
 *       fetch()} in a dedicated background thread, implementing the producer side of the
 *       producer-consumer queue that decouples IO from record emission.
 *   <li>Closes resources when the reader is closed or when {@link #wakeUp()} is called.
 * </ul>
 *
 * <h2>Producer-consumer queue</h2>
 *
 * <p>Records returned by {@link #fetch()} are placed into a {@code FutureCompletingBlockingQueue}
 * managed by the enclosing {@code SingleThreadFetcherManager}. The main reader thread in {@link
 * JdbcSourceReader} consumes from this queue and forwards records to Flink's output, forming the
 * classic producer-consumer pattern that keeps IO and downstream processing fully decoupled.
 */
public class JdbcSplitReader implements SplitReader<RowData, JdbcSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSplitReader.class);

    private final JdbcPlusOptions options;
    private final JdbcDialect dialect;
    private final JdbcRowConverter rowConverter;

    /** Queue of splits received from the enumerator, not yet started. */
    private final Deque<JdbcSourceSplit> splitQueue = new ArrayDeque<>();

    // ── Currently open resources (at most one split being read at a time) ──

    private Connection connection;
    private PreparedStatement currentStatement;
    private ResultSet currentResultSet;
    private String currentSplitId;

    /**
     * Set to {@code true} by {@link #wakeUp()} to interrupt a blocked {@link #fetch()} call and
     * allow the reader to shut down cleanly.
     */
    private volatile boolean closed = false;

    /**
     * Guards against producing multiple overlapping batches for the same split.
     *
     * <p>The fetcher thread calls {@link #fetch()} in a tight loop. Without this flag, a second
     * batch would be created for the same (still-open) ResultSet before the main reader thread has
     * called {@link JdbcResultSetRecords#recycle()} on the first one. When the main thread later
     * processes that second batch, the split has already been removed from {@code splitStates} →
     * "Have records for a split that was not registered".
     *
     * <p>Set to {@code true} just before returning a real batch; cleared to {@code false} inside
     * the {@code onClose} callback invoked by {@link JdbcResultSetRecords#recycle()}. The {@code
     * volatile} write/read pair provides the JMM happens-before guarantee so that {@code
     * currentResultSet = null} (written by the main thread before clearing this flag) is visible to
     * the fetcher thread when it resumes.
     */
    private volatile boolean batchInFlight = false;

    public JdbcSplitReader(JdbcPlusOptions options, JdbcDialect dialect, RowType rowType) {
        this.options = options;
        this.dialect = dialect;
        this.rowConverter = new JdbcRowConverter(rowType);
    }

    // -------------------------------------------------------------------------
    // SplitReader
    // -------------------------------------------------------------------------

    /**
     * Returns the next batch of records.
     *
     * <p>If a ResultSet is already open (previous call did not exhaust it), continues reading from
     * it. Otherwise opens the next pending split.
     *
     * <p>Returns an empty batch when no splits are pending, signalling the fetcher manager that it
     * should wait for more splits or shut down.
     */
    /**
     * Returns the next batch of records.
     *
     * <p>Exactly one {@link JdbcResultSetRecords} batch is "in flight" at a time ({@link
     * #batchInFlight} == true). While a batch has been handed to the framework but not yet {@link
     * JdbcResultSetRecords#recycle() recycled}, subsequent {@code fetch()} calls return an empty
     * batch so the fetcher thread does not race ahead and produce a second batch for the same
     * split. Once {@code recycle()} is called by the main reader thread, {@link #batchInFlight} is
     * cleared and the next split can be opened.
     */
    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        if (closed) {
            return JdbcResultSetRecords.empty();
        }

        // A real batch is already being consumed by the main reader thread.
        // Return empty until recycle() clears the flag.
        if (batchInFlight) {
            return JdbcResultSetRecords.empty();
        }

        // Nothing queued yet – let the fetcher thread idle.
        if (splitQueue.isEmpty()) {
            return JdbcResultSetRecords.empty();
        }

        try {
            openNextSplit();
        } catch (SQLException e) {
            throw new IOException("JDBC error while opening split " + currentSplitId, e);
        }

        batchInFlight = true;
        return new JdbcResultSetRecords(
                currentSplitId,
                currentResultSet,
                rowConverter,
                () -> {
                    // Runs on the main reader thread after all records are consumed.
                    // The volatile write to batchInFlight establishes happens-before so
                    // the fetcher thread sees currentResultSet = null (set in the call below).
                    onCurrentSplitFinished();
                    batchInFlight = false;
                });
    }

    /**
     * Receives new splits from the enumerator and queues them for processing. Only {@link
     * SplitsAddition} events are relevant; removals are ignored for batch.
     */
    @Override
    public void handleSplitsChanges(SplitsChange<JdbcSourceSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            List<JdbcSourceSplit> added = (List<JdbcSourceSplit>) splitsChange.splits();
            LOG.debug("Received {} new splits.", added.size());
            splitQueue.addAll(added);
        }
    }

    /**
     * Signals that reading should be interrupted (e.g. on job cancellation). Closes the current
     * ResultSet and marks the reader as closed.
     */
    @Override
    public void wakeUp() {
        closed = true;
        closeCurrentResources();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        closeCurrentResources();
        closeConnection();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Opens a JDBC query for the next split from the queue. Skips already-read rows using the
     * split's {@code offset}.
     */
    private void openNextSplit() throws SQLException {
        JdbcSourceSplit split = splitQueue.poll();
        if (split == null) {
            return; // defensive; caller already checks queue is non-empty
        }
        currentSplitId = split.splitId();
        ensureConnectionOpen();

        String sql = dialect.buildSplitScanQuery(split, "*", options.getFetchSize());
        LOG.debug("Opening split {}: {}", currentSplitId, sql);

        currentStatement =
                connection.prepareStatement(
                        sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        currentStatement.setFetchSize(options.getFetchSize());

        // Bind WHERE parameters: start (if present), then end (if present).
        int paramIdx = 1;
        if (split.hasStart()) {
            currentStatement.setObject(paramIdx++, split.getSplitStart());
        }
        if (split.hasEnd()) {
            currentStatement.setObject(paramIdx, split.getSplitEnd());
        }

        currentResultSet = currentStatement.executeQuery();
    }

    /**
     * Called by the {@link JdbcResultSetRecords#recycle()} callback when the ResultSet is
     * exhausted. Nulls out current-resource fields so the next {@link #fetch()} call knows to open
     * the next split.
     */
    private void onCurrentSplitFinished() {
        closeCurrentResources();
        currentSplitId = null;
    }

    private void closeCurrentResources() {
        if (currentResultSet != null) {
            try {
                currentResultSet.close();
            } catch (SQLException e) {
                /* best-effort */
            }
            currentResultSet = null;
        }
        if (currentStatement != null) {
            try {
                currentStatement.close();
            } catch (SQLException e) {
                /* best-effort */
            }
            currentStatement = null;
        }
    }

    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                /* best-effort */
            }
            connection = null;
        }
    }

    private void ensureConnectionOpen() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = openConnection();
        }
    }

    private Connection openConnection() throws SQLException {
        LOG.info("Opening JDBC connection to {}", options.getUrl());
        try {
            Class.forName(options.resolveDriverName());
        } catch (ClassNotFoundException e) {
            throw new SQLException(
                    "JDBC driver class not found: " + options.resolveDriverName(), e);
        }
        Properties props = new Properties();
        if (options.getUsername() != null) props.setProperty("user", options.getUsername());
        if (options.getPassword() != null) props.setProperty("password", options.getPassword());
        props.setProperty("connectTimeout", String.valueOf(options.getConnectionTimeoutMs()));

        Connection conn = DriverManager.getConnection(options.getUrl(), props);
        conn.setAutoCommit(false); // required for cursor-based fetching in some drivers
        return conn;
    }
}
