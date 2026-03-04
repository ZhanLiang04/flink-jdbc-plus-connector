package org.apache.flink.connector.jdbc.plus.dialect;

import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;

/**
 * MySQL / MariaDB dialect.
 *
 * <p>Uses back-tick identifier quoting and MySQL-specific {@code LIMIT offset, count} syntax for
 * chunk-boundary discovery.
 */
public class MySqlDialect implements JdbcDialect {

    private static final long serialVersionUID = 1L;

    public static final MySqlDialect INSTANCE = new MySqlDialect();

    @Override
    public String quoteIdentifier(String identifier) {
        return '`' + identifier.replace("`", "``") + '`';
    }

    // -------------------------------------------------------------------------
    // Split discovery
    // -------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     *
     * <p>MySQL example when start is non-null:
     *
     * <pre>
     *   SELECT `id` FROM `mydb`.`orders`
     *   WHERE `id` > ?
     *   ORDER BY `id`
     *   LIMIT 8096, 1
     * </pre>
     */
    @Override
    public String buildNextBoundaryQuery(
            String fullTableName, String splitKeyColumn, Object start, int chunkSize) {
        String quotedTable = quoteFullTableName(fullTableName);
        String quotedKey = quoteIdentifier(splitKeyColumn);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(quotedKey).append(" FROM ").append(quotedTable);
        if (start != null) {
            sb.append(" WHERE ").append(quotedKey).append(" > ?");
        }
        sb.append(" ORDER BY ").append(quotedKey).append(" LIMIT ").append(chunkSize).append(", 1");
        return sb.toString();
    }

    @Override
    public String buildMinMaxQuery(String fullTableName, String splitKeyColumn) {
        String quotedTable = quoteFullTableName(fullTableName);
        String quotedKey = quoteIdentifier(splitKeyColumn);
        return "SELECT MIN(" + quotedKey + "), MAX(" + quotedKey + ") FROM " + quotedTable;
    }

    // -------------------------------------------------------------------------
    // Chunk data read
    // -------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     *
     * <p>Example (middle chunk, cold start):
     *
     * <pre>
     *   SELECT * FROM `mydb`.`orders`
     *   WHERE `id` &gt; ? AND `id` &lt;= ?
     * </pre>
     *
     * <p>Example (middle chunk resumed from checkpoint at offset 512):
     *
     * <pre>
     *   SELECT * FROM `mydb`.`orders`
     *   WHERE `id` &gt; ? AND `id` &lt;= ?
     *   LIMIT 18446744073709551615 OFFSET 512
     * </pre>
     */
    @Override
    public String buildSplitScanQuery(JdbcSourceSplit split, String columns) {
        String quotedTable = quoteFullTableName(split.getFullTableName());
        String quotedKey = quoteIdentifier(split.getSplitKeyColumn());
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(columns).append(" FROM ").append(quotedTable);

        boolean hasStart = split.hasStart();
        boolean hasEnd = split.hasEnd();

        if (hasStart || hasEnd) {
            sb.append(" WHERE ");
            if (hasStart && hasEnd) {
                sb.append(quotedKey)
                        .append(" > ?")
                        .append(" AND ")
                        .append(quotedKey)
                        .append(" <= ?");
            } else if (hasStart) {
                sb.append(quotedKey).append(" > ?");
            } else {
                sb.append(quotedKey).append(" <= ?");
            }
        }

        // OFFSET is only appended when resuming from a checkpoint (offset > 0).
        // MySQL requires LIMIT before OFFSET; we use the maximum unsigned BIGINT as a no-op cap.
        if (split.getOffset() > 0) {
            sb.append(" LIMIT 18446744073709551615 OFFSET ").append(split.getOffset());
        }

        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // Schema / metadata discovery
    // -------------------------------------------------------------------------

    @Override
    public String buildTableListQuery() {
        return "SELECT TABLE_NAME FROM information_schema.TABLES "
                + "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' "
                + "ORDER BY TABLE_NAME";
    }

    @Override
    public String buildPrimaryKeyQuery(String database, String tableName) {
        return "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                + "WHERE TABLE_SCHEMA = '"
                + escapeSql(database)
                + "' "
                + "  AND TABLE_NAME  = '"
                + escapeSql(tableName)
                + "' "
                + "  AND CONSTRAINT_NAME = 'PRIMARY' "
                + "ORDER BY ORDINAL_POSITION "
                + "LIMIT 1";
    }

    @Override
    public String buildColumnMetaQuery(String database, String tableName) {
        return "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE "
                + "FROM information_schema.COLUMNS "
                + "WHERE TABLE_SCHEMA = '"
                + escapeSql(database)
                + "' "
                + "  AND TABLE_NAME  = '"
                + escapeSql(tableName)
                + "' "
                + "ORDER BY ORDINAL_POSITION";
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Converts "database.tableName" to `` `database`.`tableName` ``. Handles already-dot-separated
     * or plain names gracefully.
     */
    public String quoteFullTableName(String fullTableName) {
        int dot = fullTableName.indexOf('.');
        if (dot < 0) {
            return quoteIdentifier(fullTableName);
        }
        String db = fullTableName.substring(0, dot);
        String tbl = fullTableName.substring(dot + 1);
        return quoteIdentifier(db) + "." + quoteIdentifier(tbl);
    }

    private static String escapeSql(String s) {
        return s == null ? "" : s.replace("'", "\\'");
    }
}
