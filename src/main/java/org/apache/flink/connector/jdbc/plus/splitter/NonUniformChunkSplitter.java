package org.apache.flink.connector.jdbc.plus.splitter;

import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.plus.table.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Splits a JDBC table into evenly-sized chunks based on <em>actual data distribution</em> rather
 * than arithmetic range arithmetic.
 *
 * <h2>Why this matters</h2>
 *
 * <p>The native Flink JDBC connector divides {@code [min, max]} into N equal numeric intervals.
 * When data is not uniformly distributed (e.g. an auto-increment primary key with large gaps, or a
 * string column) this produces severely unbalanced chunks — some may contain millions of rows while
 * others are empty.
 *
 * <h2>Algorithm (inspired by Debezium / MySQL CDC ChunkSplitter)</h2>
 *
 * <ol>
 *   <li>Query {@code MIN(splitKey), MAX(splitKey)} from the table. Return a single full-table split
 *       if the table is empty.
 *   <li>Start with {@code chunkStart = null} (unbounded lower bound).
 *   <li>Query the <em>N-th next row</em> using:
 *       <pre>
 *       SELECT splitKey FROM table
 *       WHERE  splitKey &gt; chunkStart   -- omitted for first chunk
 *       ORDER BY splitKey
 *       LIMIT chunkSize, 1             -- skip chunkSize rows, return the (chunkSize+1)-th
 *       </pre>
 *   <li>If the query returns a value {@code b}:
 *       <ul>
 *         <li>Emit split {@code (chunkStart, b]} → approximately {@code chunkSize} rows.
 *         <li>Set {@code chunkStart = b} and repeat from step 3.
 *       </ul>
 *   <li>If the query returns nothing, the remaining rows fit in one final split: emit {@code
 *       (chunkStart, ∞)}.
 * </ol>
 *
 * <p>This algorithm works for <strong>any orderable column type</strong> — integers, strings,
 * timestamps — without requiring the user to know or set min/max values in advance.
 *
 * <h2>Split ID convention</h2>
 *
 * <pre>{@code database.tableName#<index>}</pre>
 */
public class NonUniformChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(NonUniformChunkSplitter.class);

    private final JdbcDialect dialect;
    private final int chunkSize;

    public NonUniformChunkSplitter(JdbcDialect dialect, int chunkSize) {
        this.dialect = dialect;
        this.chunkSize = chunkSize;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Produces a list of {@link JdbcSourceSplit}s for the given table.
     *
     * @param connection an open JDBC connection (not closed by this method)
     * @param tableInfo table metadata including the split key column
     * @return ordered list of non-overlapping, collectively-exhaustive splits
     */
    public List<JdbcSourceSplit> split(Connection connection, TableInfo tableInfo)
            throws SQLException {

        String fullTable = tableInfo.getFullTableName();
        String splitKey = tableInfo.getSplitKeyColumn();

        LOG.info(
                "Starting non-uniform chunk split for table={}, splitKey={}, chunkSize={}",
                fullTable,
                splitKey,
                chunkSize);

        // Step 1 – find min and max to handle edge cases.
        Object[] minMax = queryMinMax(connection, fullTable, splitKey);
        Object minVal = minMax[0];
        Object maxVal = minMax[1];

        if (minVal == null) {
            LOG.info("Table {} is empty; returning a single full-table split.", fullTable);
            // Empty table: one split with no WHERE condition on split key.
            return Collections.singletonList(makeSplit(fullTable, splitKey, 0, null, null));
        }

        LOG.debug("Table {} split-key range: [{}, {}]", fullTable, minVal, maxVal);

        // Step 2 – iteratively find chunk boundaries.
        List<JdbcSourceSplit> splits = new ArrayList<>();
        Comparable<?> chunkStart = null; // null → unbounded (first chunk)

        while (true) {
            // Query the row at position chunkSize from chunkStart.
            Comparable<?> nextBoundary =
                    queryNextBoundary(connection, fullTable, splitKey, chunkStart);

            if (nextBoundary == null) {
                // No (chunkSize+1)-th row → current chunk is the last one.
                splits.add(makeSplit(fullTable, splitKey, splits.size(), chunkStart, null));
                LOG.debug(
                        "Table {} last split #{}: ({}, ∞)",
                        fullTable,
                        splits.size() - 1,
                        chunkStart);
                break;
            }

            splits.add(makeSplit(fullTable, splitKey, splits.size(), chunkStart, nextBoundary));
            LOG.debug(
                    "Table {} split #{}: ({}, {}]",
                    fullTable,
                    splits.size() - 1,
                    chunkStart,
                    nextBoundary);

            chunkStart = nextBoundary;
        }

        LOG.info("Non-uniform chunk split for {} produced {} splits.", fullTable, splits.size());
        return splits;
    }

    /**
     * Streaming variant of {@link #split}: emits splits one by one via {@code consumer} as each
     * chunk boundary is discovered, instead of collecting them all first.
     *
     * <p>This lets the enumerator start distributing splits to readers immediately, without waiting
     * for the entire table to be analysed — the key enabler for the "split-while-distribute"
     * pipeline.
     *
     * @param connection an open JDBC connection (not closed by this method)
     * @param tableInfo  table metadata including the split key column
     * @param consumer   called once per split, in key-order; must be non-blocking
     */
    public void splitStreaming(
            Connection connection, TableInfo tableInfo, Consumer<JdbcSourceSplit> consumer)
            throws SQLException {

        String fullTable = tableInfo.getFullTableName();
        String splitKey = tableInfo.getSplitKeyColumn();

        LOG.info(
                "Starting streaming split for table={}, splitKey={}, chunkSize={}",
                fullTable,
                splitKey,
                chunkSize);

        Object[] minMax = queryMinMax(connection, fullTable, splitKey);
        if (minMax[0] == null) {
            LOG.info("Table {} is empty; emitting a single full-table split.", fullTable);
            consumer.accept(makeSplit(fullTable, splitKey, 0, null, null));
            return;
        }

        int idx = 0;
        Comparable<?> chunkStart = null;
        while (true) {
            Comparable<?> nextBoundary =
                    queryNextBoundary(connection, fullTable, splitKey, chunkStart);
            if (nextBoundary == null) {
                consumer.accept(makeSplit(fullTable, splitKey, idx, chunkStart, null));
                LOG.debug("Table {} last split #{}: ({}, ∞)", fullTable, idx, chunkStart);
                break;
            }
            consumer.accept(makeSplit(fullTable, splitKey, idx, chunkStart, nextBoundary));
            LOG.debug("Table {} split #{}: ({}, {}]", fullTable, idx, chunkStart, nextBoundary);
            chunkStart = nextBoundary;
            idx++;
        }
        LOG.info("Streaming split for {} finished, {} splits emitted.", fullTable, idx + 1);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Queries {@code MIN(col), MAX(col)} from the table.
     *
     * @return two-element array {@code [min, max]}; both {@code null} if table is empty
     */
    private Object[] queryMinMax(Connection conn, String fullTable, String splitKey)
            throws SQLException {
        String sql = dialect.buildMinMaxQuery(fullTable, splitKey);
        LOG.trace("Min/max query: {}", sql);
        try (PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return new Object[] {rs.getObject(1), rs.getObject(2)};
            }
            return new Object[] {null, null};
        }
    }

    /**
     * Queries the split-key value of the row at index {@code chunkSize} (0-based) strictly after
     * {@code currentStart}.
     *
     * <p>Uses the dialect's {@link JdbcDialect#buildNextBoundaryQuery(String, String, Object, int)}
     * which emits {@code LIMIT chunkSize, 1} (skip {@code chunkSize} rows, return 1).
     *
     * @param currentStart exclusive lower bound for the scan; {@code null} → start from beginning
     * @return the boundary value, or {@code null} if fewer than {@code chunkSize+1} rows remain
     */
    private Comparable<?> queryNextBoundary(
            Connection conn, String fullTable, String splitKey, Comparable<?> currentStart)
            throws SQLException {

        String sql = dialect.buildNextBoundaryQuery(fullTable, splitKey, currentStart, chunkSize);
        LOG.trace("Next-boundary query (start={}): {}", currentStart, sql);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (currentStart != null) {
                setParameter(ps, 1, currentStart);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return (Comparable<?>) rs.getObject(1);
                }
                return null;
            }
        }
    }

    /**
     * Sets a prepared statement parameter, handling common split-key types. Uses {@link
     * PreparedStatement#setObject(int, Object)} as the fallback so that any JDBC-compatible type
     * works without explicit handling.
     */
    private void setParameter(PreparedStatement ps, int idx, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, java.sql.Types.NULL);
        } else {
            // setObject lets the JDBC driver perform the correct type mapping
            // for Integer, Long, String, BigDecimal, Date, Timestamp, etc.
            ps.setObject(idx, value);
        }
    }

    /**
     * Factory method to create a {@link JdbcSourceSplit} with a generated ID.
     *
     * @param idx zero-based index within the table's split list (used in the ID)
     * @param start exclusive lower bound; {@code null} → first chunk
     * @param end inclusive upper bound; {@code null} → last chunk
     */
    private JdbcSourceSplit makeSplit(
            String fullTableName,
            String splitKeyColumn,
            int idx,
            Comparable<?> start,
            Comparable<?> end) {
        String splitId = fullTableName + "#" + idx;
        return new JdbcSourceSplit(splitId, fullTableName, splitKeyColumn, start, end);
    }
}
