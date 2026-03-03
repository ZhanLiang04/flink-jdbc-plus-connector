package org.apache.flink.connector.jdbc.plus.dialect;

import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;

import java.io.Serializable;

/**
 * Abstraction layer for database-specific SQL syntax.
 *
 * <p>Implementations provide methods to build SQL statements for:
 *
 * <ul>
 *   <li>Non-uniform chunk boundary discovery
 *   <li>Split scan queries (SELECT ... WHERE split_key ...)
 *   <li>Table / primary-key discovery
 * </ul>
 */
public interface JdbcDialect extends Serializable {

    /**
     * Wraps an identifier (table name, column name) in dialect-specific quoting, e.g. back-ticks
     * for MySQL, double-quotes for ANSI SQL.
     */
    String quoteIdentifier(String identifier);

    /**
     * Builds the SQL used to discover the next chunk boundary during non-uniform chunk splitting.
     *
     * <p>The query must return the value of {@code splitKey} at position {@code chunkSize}
     * (0-based) from the current {@code start} position:
     *
     * <pre>
     *   SELECT `splitKey` FROM `db`.`table`
     *   WHERE `splitKey` &gt; start          -- omitted when start IS NULL (first chunk)
     *   ORDER BY `splitKey`
     *   LIMIT chunkSize, 1                  -- skip chunkSize rows, return 1
     * </pre>
     *
     * If the query returns no rows the caller knows we are in the last chunk.
     *
     * @param fullTableName "database.tableName"
     * @param splitKeyColumn split key column name
     * @param start current lower bound (exclusive); {@code null} → first chunk
     * @param chunkSize number of rows per chunk
     */
    String buildNextBoundaryQuery(
            String fullTableName, String splitKeyColumn, Object start, int chunkSize);

    /**
     * Builds the SQL used to find the MIN and MAX of the split key column.
     *
     * <pre>SELECT MIN(`col`), MAX(`col`) FROM `db`.`table`</pre>
     */
    String buildMinMaxQuery(String fullTableName, String splitKeyColumn);

    /**
     * Builds the data-read query for a single {@link JdbcSourceSplit}.
     *
     * <p>The WHERE clause encodes the chunk boundary:
     *
     * <ul>
     *   <li>No bounds (sole split): no WHERE on split key
     *   <li>Only end: {@code WHERE splitKey <= ?}
     *   <li>Only start: {@code WHERE splitKey > ?}
     *   <li>Both: {@code WHERE splitKey > ? AND splitKey <= ?}
     * </ul>
     *
     * When {@code split.getOffset() > 0} the query appends {@code OFFSET} so the reader can skip
     * already-emitted rows after a failover.
     *
     * @param split the split to read
     * @param columns comma-separated column list, or {@code "*"}
     * @param fetchSize JDBC fetch-size hint (appended as LIMIT when > 0); 0 = no limit
     */
    String buildSplitScanQuery(JdbcSourceSplit split, String columns, int fetchSize);

    /**
     * Builds the SQL to list all table names in a database.
     *
     * <pre>SELECT TABLE_NAME FROM information_schema.TABLES
     *   WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'</pre>
     */
    String buildTableListQuery();

    /**
     * Builds the SQL to find the primary-key columns for a specific table.
     *
     * @param database schema/database name
     * @param tableName table name (without database prefix)
     */
    String buildPrimaryKeyQuery(String database, String tableName);

    /** Builds the SQL to retrieve column metadata (name, JDBC type) for a table. */
    String buildColumnMetaQuery(String database, String tableName);
}
