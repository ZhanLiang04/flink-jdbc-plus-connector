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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

/**
 * JobManager-side component of the FLIP-27 source that discovers and assigns {@link
 * JdbcSourceSplit}s to {@code SourceReader}s running on TaskManagers.
 *
 * <h2>Split-while-distribute pipeline</h2>
 *
 * <p>This enumerator implements a <em>producer/consumer pipeline</em> so that splits are
 * distributed to readers as soon as they are ready, without waiting for all splits to be
 * enumerated first:
 *
 * <ol>
 *   <li>{@link #start()} submits table/chunk discovery as a single {@link
 *       SplitEnumeratorContext#callAsync} call. The callable runs on a background thread and
 *       streams each split into a {@link LinkedBlockingQueue} the moment its boundary is found.
 *   <li>After each enqueue the background thread calls {@link #scheduleDrainIfNeeded()}: if no
 *       drain is already pending it submits a lightweight no-op {@code callAsync} whose callback
 *       ({@link #drainQueueAndFulfillBacklog}) runs on the JM main thread, drains all buffered
 *       splits into {@link #pendingSplits}, and assigns them to any waiting readers.
 *   <li>When a TM calls {@link #handleSplitRequest} the enumerator eagerly drains the shared queue
 *       before looking in {@link #pendingSplits}, so splits are delivered with minimal latency.
 *   <li>When discovery finishes, {@link #onDiscoveryComplete} performs a final drain, sets
 *       {@link #allSplitsCreated}, and signals "no more splits" to any still-waiting readers.
 * </ol>
 *
 * <h2>Thread safety</h2>
 *
 * <p>{@link #splitQueue} is the only object shared between the background thread and the JM main
 * thread. It is a {@link LinkedBlockingQueue} (thread-safe). All other state ({@link
 * #pendingSplits}, {@link #splitRequestBacklog}, {@link #allSplitsCreated}) is accessed
 * exclusively on the JM main thread — either inside a {@code callAsync} handler or inside the
 * {@link SplitEnumerator} interface methods, which Flink guarantees to run on the coordinator
 * thread.
 *
 * <p>{@link #drainScheduled} is an {@link AtomicBoolean} that acts as a "coalescing" guard: many
 * splits can be produced between two consecutive JM-thread drain runs, but only one drain callback
 * is ever queued at a time, preventing mailbox flooding.
 */
public class JdbcSourceEnumerator
        implements SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceEnumerator.class);

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private final JdbcPlusOptions options;
    private final JdbcDialect dialect;

    // ── JM-thread-only state ─────────────────────────────────────────────────

    /** Splits ready to be assigned but not yet sent to any reader. */
    private final Queue<JdbcSourceSplit> pendingSplits;

    /**
     * Tables that still need to be split. Populated on restore from checkpoint so that tables
     * whose splits were not yet fully emitted can be re-discovered.
     */
    private final List<String> remainingTables;

    /**
     * Subtask IDs waiting for a split that is not yet available (discovery in progress). Drained
     * whenever new splits arrive or discovery completes.
     */
    private final Map<Integer, String> splitRequestBacklog;

    /** True once all table splits have been enumerated and transferred to {@link #pendingSplits}. */
    private volatile boolean allSplitsCreated;

    // ── Cross-thread bridge ──────────────────────────────────────────────────

    /**
     * Pipe between the background discovery thread (producer) and the JM main thread (consumer).
     * The background thread offers splits here; the JM thread drains them into {@link
     * #pendingSplits}.
     */
    private final LinkedBlockingQueue<JdbcSourceSplit> splitQueue;

    /**
     * Guards against enqueueing redundant drain callbacks. The background thread does a CAS from
     * {@code false → true} before scheduling a drain; the drain handler resets it to {@code false}
     * when it starts. This way many splits can be batched into a single drain run.
     */
    private final AtomicBoolean drainScheduled;

    // ── Constructors ─────────────────────────────────────────────────────────

    public JdbcSourceEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context,
            JdbcPlusOptions options,
            JdbcDialect dialect) {
        this(context, options, dialect, null);
    }

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
        this.splitQueue = new LinkedBlockingQueue<>();
        this.drainScheduled = new AtomicBoolean(false);

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
     * Kicks off asynchronous split discovery.
     *
     * <p>The JDBC work runs on a background thread via {@link SplitEnumeratorContext#callAsync}.
     * Each split is published to {@link #splitQueue} the moment it is computed; a drain callback
     * is then coalesced-scheduled on the JM main thread so splits flow to readers immediately.
     */
    @Override
    public void start() {
        if (allSplitsCreated) {
            LOG.info("All splits already created (restored from checkpoint); skipping discovery.");
            fulfillBacklog();
            return;
        }
        LOG.info("Starting async split-while-distribute discovery.");
        context.callAsync(this::discoverAndPublishSplits, this::onDiscoveryComplete);
    }

    /**
     * Called when a TaskManager subtask requests a new split.
     *
     * <p>Eagerly drains {@link #splitQueue} before consulting {@link #pendingSplits} so that
     * splits produced by the background thread since the last drain are not delayed.
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Split request from subtask {} (host={})", subtaskId, requesterHostname);
        drainSharedQueue();
        assignNextSplitOrEnqueue(subtaskId);
    }

    /**
     * Re-queues splits returned by a failed TaskManager subtask to the front of the pending queue
     * so they are retried before newly discovered splits.
     */
    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        LOG.info("Subtask {} failed; re-queuing {} splits.", subtaskId, splits.size());
        List<JdbcSourceSplit> reversed = new ArrayList<>(splits);
        Collections.reverse(reversed);
        reversed.forEach(s -> ((ArrayDeque<JdbcSourceSplit>) pendingSplits).addFirst(s));
    }

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
        // Drain cross-thread queue so the snapshot is consistent.
        drainSharedQueue();
        return new JdbcSourceEnumeratorState(
                new ArrayList<>(pendingSplits), new ArrayList<>(remainingTables), allSplitsCreated);
    }

    @Override
    public void close() {
        // The background callAsync thread is managed by Flink; nothing to close here.
    }

    // -------------------------------------------------------------------------
    // Background thread — produces splits into splitQueue
    // -------------------------------------------------------------------------

    /**
     * Runs on a Flink-managed background thread. Connects to the database, discovers all tables,
     * and for each table calls {@link NonUniformChunkSplitter#splitStreaming} which invokes the
     * provided consumer once per split. The consumer immediately offers the split to {@link
     * #splitQueue} and triggers a coalesced drain on the JM main thread.
     *
     * <p>No JM-thread state is accessed here — only {@link #splitQueue} (thread-safe) and {@link
     * #drainScheduled} (atomic).
     */
    private Void discoverAndPublishSplits() throws Exception {
        LOG.info("Background discovery thread started; connecting to {}", options.getUrl());
        try (Connection conn = openConnection()) {
            TableDiscovery discovery = new TableDiscovery(options, dialect);
            List<TableInfo> tables = discovery.discoverTables(conn);
            LOG.info("Discovered {} table(s) to split.", tables.size());

            NonUniformChunkSplitter splitter =
                    new NonUniformChunkSplitter(dialect, options.getChunkSize());

            for (TableInfo table : tables) {
                LOG.info("Streaming splits for table: {}", table.getFullTableName());
                splitter.splitStreaming(
                        conn,
                        table,
                        split -> {
                            splitQueue.offer(split);
                            scheduleDrainIfNeeded();
                        });
            }
        }
        LOG.info("Background discovery thread finished.");
        return null;
    }

    /**
     * Schedules a single drain callback on the JM main thread if one is not already pending.
     *
     * <p>Called from the background thread after each {@link #splitQueue} offer. The
     * compare-and-set on {@link #drainScheduled} ensures that many rapid offers only ever queue
     * one drain callback, preventing mailbox flooding.
     */
    private void scheduleDrainIfNeeded() {
        if (drainScheduled.compareAndSet(false, true)) {
            context.callAsync(
                    () -> null,
                    (ignored, err) -> {
                        drainScheduled.set(false);
                        if (err != null) {
                            LOG.warn("Unexpected error in drain scheduling callback", err);
                            return;
                        }
                        drainQueueAndFulfillBacklog();
                    });
        }
    }

    // -------------------------------------------------------------------------
    // JM main thread — consumes splits from splitQueue, assigns to readers
    // -------------------------------------------------------------------------

    /**
     * Called on the JM main thread when {@link #discoverAndPublishSplits()} completes.
     *
     * <p>Performs a final drain of {@link #splitQueue}, marks discovery as done, and fulfils any
     * readers still in the backlog (either assigning their last split or signalling end-of-input).
     */
    private void onDiscoveryComplete(Void ignored, Throwable error) {
        if (error != null) {
            LOG.error("Split discovery failed", error);
            throw new RuntimeException("JDBC split discovery failed", error);
        }
        LOG.info("Discovery complete. Performing final drain.");
        drainSharedQueue();
        allSplitsCreated = true;
        fulfillBacklog();
    }

    /**
     * Drains all splits currently in {@link #splitQueue} into {@link #pendingSplits} and then
     * tries to satisfy any backlogged split requests.
     *
     * <p>Runs on the JM main thread (called from the coalesced drain callback).
     */
    private void drainQueueAndFulfillBacklog() {
        int drained = drainSharedQueue();
        if (drained > 0) {
            LOG.debug("Drained {} split(s) from queue; fulfilling backlog.", drained);
            fulfillBacklog();
        }
    }

    /**
     * Moves all currently available splits from the shared {@link #splitQueue} into the JM-local
     * {@link #pendingSplits}.
     *
     * @return the number of splits transferred
     */
    private int drainSharedQueue() {
        int count = 0;
        JdbcSourceSplit split;
        while ((split = splitQueue.poll()) != null) {
            pendingSplits.add(split);
            count++;
        }
        return count;
    }

    /**
     * Attempts to assign a queued split to each backlogged reader. Readers for whom no split is
     * currently available are re-parked in {@link #splitRequestBacklog} (or told "no more splits"
     * if {@link #allSplitsCreated} is true).
     */
    private void fulfillBacklog() {
        if (splitRequestBacklog.isEmpty()) {
            return;
        }
        List<Integer> waiting = new ArrayList<>(splitRequestBacklog.keySet());
        splitRequestBacklog.clear();
        for (int subtaskId : waiting) {
            assignNextSplitOrEnqueue(subtaskId);
        }
    }

    /**
     * Tries to assign the next pending split to {@code subtaskId}. If none is available and all
     * splits have been created, signals end-of-input. If discovery is still in progress, re-parks
     * the request in {@link #splitRequestBacklog}.
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
            splitRequestBacklog.put(subtaskId, "pending");
            LOG.debug("Split request from subtask {} parked (discovery in flight)", subtaskId);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
