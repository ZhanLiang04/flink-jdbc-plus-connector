package org.apache.flink.connector.jdbc.plus.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.jdbc.plus.JdbcPlusOptions;
import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.plus.splitter.NonUniformChunkSplitter;
import org.apache.flink.connector.jdbc.plus.table.TableDiscovery;
import org.apache.flink.connector.jdbc.plus.table.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import javax.annotation.Nullable;

/**
 * JobManager-side component of the FLIP-27 source that discovers and assigns {@link
 * JdbcSourceSplit}s to {@code SourceReader}s running on TaskManagers.
 *
 * <h2>JM / TM decoupling</h2>
 *
 * <p>This class runs exclusively on the JobManager. It never reads actual table data; its sole
 * responsibility is <em>split management</em>:
 *
 * <ol>
 *   <li>On {@link #start()}: schedules table/chunk discovery as an async call via {@link
 *       SplitEnumeratorContext#callAsync} so the JM's main thread is never blocked.
 *   <li>On {@link #handleSplitRequest}: fulfils pending reader requests from a shared {@link
 *       Queue}, or queues the request for later fulfillment once discovery completes.
 *   <li>On {@link #addSplitsBack}: re-queues splits that were returned by a failed TaskManager
 *       subtask.
 *   <li>On {@link #snapshotState}: serialises pending splits and remaining tables so Flink can
 *       restore the enumerator after a JobManager failure.
 * </ol>
 *
 * <h2>Communication model</h2>
 *
 * <p>Readers request splits by calling {@link SplitEnumeratorContext#sendSplitRequest()}. The
 * enumerator responds with either an assigned split or {@link
 * SplitEnumeratorContext#signalNoMoreSplits(int)}. The exchange is purely message-based with <em>no
 * shared state</em> between JM and TM — the canonical FLIP-27 producer/consumer pattern.
 */
public class JdbcSourceEnumerator
        implements SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceEnumerator.class);

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private final JdbcPlusOptions options;
    private final JdbcDialect dialect;

    // ── Mutable state (all access on JM main thread via callAsync handler) ──

    /** Splits ready to be assigned but not yet sent to any reader. */
    private final Queue<JdbcSourceSplit> pendingSplits;

    /**
     * Tables that still need to be split. Populated from the checkpoint state on restore; used to
     * re-run discovery for tables whose splits were lost.
     */
    private final List<String> remainingTables;

    /**
     * Subtask IDs waiting for a split that is not yet available (discovery in progress). When
     * discovery completes, these are fulfilled immediately.
     */
    private final Map<Integer, String> splitRequestBacklog;

    /** True once all table splits have been enumerated. */
    private volatile boolean allSplitsCreated;

    // ── Constructor for fresh start ──

    public JdbcSourceEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context,
            JdbcPlusOptions options,
            JdbcDialect dialect) {
        this(context, options, dialect, null);
    }

    // ── Constructor for restore from checkpoint ──

    public JdbcSourceEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context,
            JdbcPlusOptions options,
            JdbcDialect dialect,
            @Nullable JdbcSourceEnumeratorState restoredState) {
        this.context = context;
        this.options = options;
        this.dialect = dialect;
        this.pendingSplits = new ArrayDeque<>();
        this.remainingTables = new ArrayList<>();
        this.splitRequestBacklog = new HashMap<>();

        if (restoredState != null) {
            pendingSplits.addAll(restoredState.getPendingSplits());
            remainingTables.addAll(restoredState.getRemainingTables());
            this.allSplitsCreated = restoredState.isAllSplitsCreated();
            LOG.info(
                    "Restored enumerator state: {} pending splits, {} remaining tables",
                    pendingSplits.size(),
                    remainingTables.size());
        }
    }

    // -------------------------------------------------------------------------
    // SplitEnumerator lifecycle
    // -------------------------------------------------------------------------

    /**
     * Triggers asynchronous table and chunk discovery.
     *
     * <p>The heavy JDBC work (connecting, querying MIN/MAX, walking boundaries) is executed on a
     * background thread via {@link SplitEnumeratorContext#callAsync}. The callback {@link
     * #onSplitsDiscovered} is invoked on the JM main thread, so all state mutations are
     * thread-safe.
     */
    @Override
    public void start() {
        if (allSplitsCreated) {
            LOG.info("All splits already created (restored from checkpoint); skipping discovery.");
            return;
        }
        LOG.info("Starting async table/chunk discovery.");
        context.callAsync(this::discoverAllSplits, this::onSplitsDiscovered);
    }

    /**
     * Called when a TaskManager subtask requests a new split.
     *
     * <p>If a pending split is available, it is assigned immediately. Otherwise the request is
     * placed in the backlog and fulfilled later when discovery completes, or the subtask is told
     * "no more splits" if all work is done.
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Split request from subtask {} (host={})", subtaskId, requesterHostname);
        assignNextSplitOrEnqueue(subtaskId);
    }

    /**
     * Called when a TaskManager subtask fails and its assigned splits must be re-processed. The
     * splits are returned to the head of the pending queue so they are reassigned promptly.
     */
    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        LOG.info("Subtask {} failed; re-queuing {} splits.", subtaskId, splits.size());
        // Add to front so failed splits are retried before newly discovered ones.
        List<JdbcSourceSplit> reversed = new ArrayList<>(splits);
        Collections.reverse(reversed);
        reversed.forEach(s -> ((ArrayDeque<JdbcSourceSplit>) pendingSplits).addFirst(s));
    }

    /** No-op: for a bounded batch source there is nothing to do when a subtask restarts. */
    @Override
    public void addReader(int subtaskId) {
        LOG.debug("New reader registered: subtask {}", subtaskId);
    }

    // -------------------------------------------------------------------------
    // Checkpoint / restore
    // -------------------------------------------------------------------------

    @Override
    public JdbcSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Snapshotting enumerator state at checkpoint {}", checkpointId);
        return new JdbcSourceEnumeratorState(
                new ArrayList<>(pendingSplits), new ArrayList<>(remainingTables), allSplitsCreated);
    }

    @Override
    public void close() {
        // Nothing to close; JDBC connection is opened only during discoverAllSplits.
    }

    // -------------------------------------------------------------------------
    // Async discovery (runs on a background thread)
    // -------------------------------------------------------------------------

    /**
     * Opens a JDBC connection, discovers all tables, and splits each one. This method is executed
     * on a non-JM thread.
     *
     * @return list of all discovered splits
     */
    private List<JdbcSourceSplit> discoverAllSplits() throws Exception {
        LOG.info("Connecting to JDBC for split discovery: {}", options.getUrl());
        try (Connection conn = openConnection()) {
            TableDiscovery discovery = new TableDiscovery(options, dialect);
            List<TableInfo> tables = discovery.discoverTables(conn);

            NonUniformChunkSplitter splitter =
                    new NonUniformChunkSplitter(dialect, options.getChunkSize());

            List<JdbcSourceSplit> allSplits = new ArrayList<>();
            for (TableInfo table : tables) {
                LOG.info("Splitting table: {}", table.getFullTableName());
                List<JdbcSourceSplit> tableSplits = splitter.split(conn, table);
                allSplits.addAll(tableSplits);
                LOG.info(
                        "Table {} produced {} splits.",
                        table.getFullTableName(),
                        tableSplits.size());
            }
            return allSplits;
        }
    }

    // -------------------------------------------------------------------------
    // Callback (runs on JM main thread)
    // -------------------------------------------------------------------------

    /**
     * Invoked on the JM main thread once {@link #discoverAllSplits()} completes.
     *
     * <p>Adds all newly discovered splits to {@link #pendingSplits} and fulfils any backlogged
     * requests from readers that were waiting.
     */
    private void onSplitsDiscovered(List<JdbcSourceSplit> newSplits, Throwable error) {
        if (error != null) {
            LOG.error("Split discovery failed: {}", error.getMessage(), error);
            throw new RuntimeException("JDBC split discovery failed", error);
        }

        LOG.info("Discovery complete: {} total splits.", newSplits.size());
        pendingSplits.addAll(newSplits);
        allSplitsCreated = true;

        // Fulfil any readers that requested splits while discovery was in flight.
        for (Map.Entry<Integer, String> entry : splitRequestBacklog.entrySet()) {
            assignNextSplitOrEnqueue(entry.getKey());
        }
        splitRequestBacklog.clear();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Tries to assign the next pending split to {@code subtaskId}. If none is available and all
     * splits have been created, signals end-of-input. If discovery is still in progress, records
     * the request in the backlog.
     */
    private void assignNextSplitOrEnqueue(int subtaskId) {
        JdbcSourceSplit split = pendingSplits.poll();
        if (split != null) {
            context.assignSplit(split, subtaskId);
            LOG.debug("Assigned split {} to subtask {}", split.splitId(), subtaskId);
        } else if (allSplitsCreated) {
            context.signalNoMoreSplits(subtaskId);
            LOG.debug("No more splits for subtask {}", subtaskId);
        } else {
            // Discovery in flight; park the request until onSplitsDiscovered is called.
            splitRequestBacklog.put(subtaskId, "pending");
            LOG.debug("Split request from subtask {} parked (discovery in flight)", subtaskId);
        }
    }

    /** Opens a JDBC connection using the configured options. */
    private Connection openConnection() throws SQLException {
        try {
            Class.forName(options.resolveDriverName());
        } catch (ClassNotFoundException e) {
            throw new SQLException("JDBC driver not found: " + options.resolveDriverName(), e);
        }
        Properties props = new Properties();
        if (options.getUsername() != null) props.setProperty("user", options.getUsername());
        if (options.getPassword() != null) props.setProperty("password", options.getPassword());
        props.setProperty("connectTimeout", String.valueOf(options.getConnectionTimeoutMs()));
        return DriverManager.getConnection(options.getUrl(), props);
    }
}
