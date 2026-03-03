package org.apache.flink.connector.jdbc.plus.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * A {@link SourceSplit} representing a bounded chunk of a JDBC table.
 *
 * <p>Each split corresponds to a WHERE clause on the split key column:
 *
 * <ul>
 *   <li>First split: {@code splitKey <= splitEnd}
 *   <li>Middle split: {@code splitKey > splitStart AND splitKey <= splitEnd}
 *   <li>Last split: {@code splitKey > splitStart}
 *   <li>Sole split: no WHERE clause on split key (full table)
 * </ul>
 *
 * <p>The {@link #offset} field counts rows already emitted from this split and is updated on every
 * checkpoint so the reader can resume after a failure by skipping already-emitted rows via {@code
 * OFFSET}.
 */
public class JdbcSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    /** Globally unique identifier for this split. */
    private final String splitId;

    /** Fully-qualified table name: "database.tableName". */
    private final String fullTableName;

    /** Column used for chunk boundaries. */
    private final String splitKeyColumn;

    /**
     * Lower boundary value (exclusive), or {@code null} for the first chunk. The type must be
     * comparable and serializable (e.g. Integer, Long, String).
     */
    private final Comparable<?> splitStart;

    /** Upper boundary value (inclusive), or {@code null} for the last chunk. */
    private final Comparable<?> splitEnd;

    /**
     * Number of rows already emitted. Persisted in checkpoints to allow resumption via SQL {@code
     * LIMIT … OFFSET …}.
     */
    private final long offset;

    public JdbcSourceSplit(
            String splitId,
            String fullTableName,
            String splitKeyColumn,
            Comparable<?> splitStart,
            Comparable<?> splitEnd,
            long offset) {
        this.splitId = Objects.requireNonNull(splitId, "splitId");
        this.fullTableName = Objects.requireNonNull(fullTableName, "fullTableName");
        this.splitKeyColumn = Objects.requireNonNull(splitKeyColumn, "splitKeyColumn");
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.offset = offset;
    }

    /** Convenience constructor with zero offset (new, unread split). */
    public JdbcSourceSplit(
            String splitId,
            String fullTableName,
            String splitKeyColumn,
            Comparable<?> splitStart,
            Comparable<?> splitEnd) {
        this(splitId, fullTableName, splitKeyColumn, splitStart, splitEnd, 0L);
    }

    // -------------------------------------------------------------------------
    // SourceSplit
    // -------------------------------------------------------------------------

    @Override
    public String splitId() {
        return splitId;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public String getFullTableName() {
        return fullTableName;
    }

    public String getSplitKeyColumn() {
        return splitKeyColumn;
    }

    public Comparable<?> getSplitStart() {
        return splitStart;
    }

    public Comparable<?> getSplitEnd() {
        return splitEnd;
    }

    public long getOffset() {
        return offset;
    }

    public boolean hasStart() {
        return splitStart != null;
    }

    public boolean hasEnd() {
        return splitEnd != null;
    }

    /**
     * Returns a new split identical to this one but with the given offset. Used when creating
     * checkpoint snapshots.
     */
    public JdbcSourceSplit withOffset(long newOffset) {
        return new JdbcSourceSplit(
                splitId, fullTableName, splitKeyColumn, splitStart, splitEnd, newOffset);
    }

    @Override
    public String toString() {
        return "JdbcSourceSplit{"
                + "id='"
                + splitId
                + '\''
                + ", table='"
                + fullTableName
                + '\''
                + ", splitKey='"
                + splitKeyColumn
                + '\''
                + ", start="
                + splitStart
                + ", end="
                + splitEnd
                + ", offset="
                + offset
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JdbcSourceSplit)) return false;
        JdbcSourceSplit that = (JdbcSourceSplit) o;
        return offset == that.offset
                && Objects.equals(splitId, that.splitId)
                && Objects.equals(fullTableName, that.fullTableName)
                && Objects.equals(splitKeyColumn, that.splitKeyColumn)
                && Objects.equals(splitStart, that.splitStart)
                && Objects.equals(splitEnd, that.splitEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, fullTableName, splitKeyColumn, splitStart, splitEnd, offset);
    }
}
