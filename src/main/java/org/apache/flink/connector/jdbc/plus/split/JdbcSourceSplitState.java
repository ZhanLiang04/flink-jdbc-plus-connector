package org.apache.flink.connector.jdbc.plus.split;

/**
 * Mutable state for a {@link JdbcSourceSplit} inside a running {@code SourceReader}.
 *
 * <p>{@link org.apache.flink.api.connector.source.SourceReader} implementations based on {@code
 * SourceReaderBase} keep one {@code SplitState} object per in-flight split. The state is mutated by
 * the {@code RecordEmitter} on every emitted record (updating {@link #currentOffset}) and
 * snapshotted to a new immutable {@link JdbcSourceSplit} on checkpoint via {@link
 * #toSourceSplit()}.
 */
public class JdbcSourceSplitState {

    private final JdbcSourceSplit split;
    private long currentOffset;

    public JdbcSourceSplitState(JdbcSourceSplit split) {
        this.split = split;
        this.currentOffset = split.getOffset();
    }

    public JdbcSourceSplit getUnderlyingSplit() {
        return split;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void incrementOffset() {
        this.currentOffset++;
    }

    /**
     * Materialises this mutable state into an immutable {@link JdbcSourceSplit} snapshot suitable
     * for inclusion in a Flink checkpoint.
     */
    public JdbcSourceSplit toSourceSplit() {
        return split.withOffset(currentOffset);
    }
}
