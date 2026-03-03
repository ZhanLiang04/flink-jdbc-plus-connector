package org.apache.flink.connector.jdbc.plus.table;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata describing a single JDBC table that will be read.
 *
 * <p>Carries the database/table names, the split key column resolved for this specific table, and
 * the SQL type of that column (used during chunk-boundary queries and WHERE-clause generation).
 */
public class TableInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Fully-qualified table name: "database.tableName". */
    private final String fullTableName;

    private final String database;
    private final String tableName;

    /**
     * Column used to partition this table into chunks. This is either the user-specified column or
     * the first PK column discovered via JDBC metadata / INFORMATION_SCHEMA.
     */
    private final String splitKeyColumn;

    /**
     * JDBC type constant ({@link java.sql.Types}) of the split key column. Used to cast boundary
     * values correctly when building WHERE clauses.
     */
    private final int splitKeyJdbcType;

    public TableInfo(
            String database, String tableName, String splitKeyColumn, int splitKeyJdbcType) {
        this.database = Objects.requireNonNull(database, "database");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.fullTableName = database + "." + tableName;
        this.splitKeyColumn = Objects.requireNonNull(splitKeyColumn, "splitKeyColumn");
        this.splitKeyJdbcType = splitKeyJdbcType;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullTableName() {
        return fullTableName;
    }

    public String getSplitKeyColumn() {
        return splitKeyColumn;
    }
    public int getSplitKeyJdbcType() {
        return splitKeyJdbcType;
    }

    @Override
    public String toString() {
        return "TableInfo{table=" + fullTableName + ", splitKey=" + splitKeyColumn + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableInfo)) return false;
        TableInfo that = (TableInfo) o;
        return Objects.equals(fullTableName, that.fullTableName)
                && Objects.equals(splitKeyColumn, that.splitKeyColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullTableName, splitKeyColumn);
    }
}
