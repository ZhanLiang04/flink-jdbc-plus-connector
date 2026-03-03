package org.apache.flink.connector.jdbc.plus.enumerator;

import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Checkpoint state of {@link JdbcSourceEnumerator}.
 *
 * <p>Captures:
 *
 * <ul>
 *   <li>{@link #pendingSplits} – splits discovered but not yet assigned to any reader.
 *   <li>{@link #remainingTables} – fully-qualified table names for which chunk discovery has not
 *       yet started. Populated when table discovery is done lazily (or after a failover before
 *       discovery completes).
 *   <li>{@link #allSplitsCreated} – {@code true} once every table has been fully split so the
 *       enumerator knows it can signal "no more splits" to idle readers.
 * </ul>
 */
public class JdbcSourceEnumeratorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<JdbcSourceSplit> pendingSplits;
    private final List<String> remainingTables;
    private final boolean allSplitsCreated;

    public JdbcSourceEnumeratorState(
            List<JdbcSourceSplit> pendingSplits,
            List<String> remainingTables,
            boolean allSplitsCreated) {
        this.pendingSplits = Collections.unmodifiableList(new ArrayList<>(pendingSplits));
        this.remainingTables = Collections.unmodifiableList(new ArrayList<>(remainingTables));
        this.allSplitsCreated = allSplitsCreated;
    }

    public List<JdbcSourceSplit> getPendingSplits() {
        return pendingSplits;
    }

    public List<String> getRemainingTables() {
        return remainingTables;
    }

    public boolean isAllSplitsCreated() {
        return allSplitsCreated;
    }

    @Override
    public String toString() {
        return "JdbcSourceEnumeratorState{"
                + "pendingSplits="
                + pendingSplits.size()
                + ", remainingTables="
                + remainingTables
                + ", allSplitsCreated="
                + allSplitsCreated
                + '}';
    }
}
