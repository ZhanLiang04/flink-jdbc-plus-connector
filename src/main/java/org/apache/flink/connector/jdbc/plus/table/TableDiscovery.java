package org.apache.flink.connector.jdbc.plus.table;

import org.apache.flink.connector.jdbc.plus.JdbcPlusOptions;
import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Discovers the set of JDBC tables to be read.
 *
 * <h2>Two modes</h2>
 *
 * <dl>
 *   <dt>Explicit list
 *   <dd>{@link JdbcPlusOptions#getTableList()} – a fixed, comma-separated list of table names. Each
 *       entry is either plain {@code tableName} (the configured {@link
 *       JdbcPlusOptions#getDatabase()} is used as the schema) or the fully qualified {@code
 *       database.tableName}.
 *   <dt>Regex pattern
 *   <dd>{@link JdbcPlusOptions#getTablePattern()} – a Java regular expression matched against every
 *       {@code database.tableName} returned by {@code information_schema.TABLES}. This allows
 *       wildcards such as {@code "mydb\\.order_.*"} without prior knowledge of all table names.
 * </dl>
 *
 * <h2>Split-key resolution</h2>
 *
 * <p>For each discovered table the split key is resolved in this order:
 *
 * <ol>
 *   <li>Global override: {@link JdbcPlusOptions#getSplitKeyColumn()}.
 *   <li>Auto-detect: the first column in the table's {@code PRIMARY KEY} constraint as reported by
 *       {@code information_schema.KEY_COLUMN_USAGE}.
 *   <li>Fallback: the first column in the table (not recommended for large tables).
 * </ol>
 */
public class TableDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(TableDiscovery.class);

    private final JdbcPlusOptions options;
    private final JdbcDialect dialect;

    public TableDiscovery(JdbcPlusOptions options, JdbcDialect dialect) {
        this.options = options;
        this.dialect = dialect;
    }

    // -------------------------------------------------------------------------
    // Public entry point
    // -------------------------------------------------------------------------

    /**
     * Returns the list of {@link TableInfo} objects to be read, with split keys resolved.
     *
     * @param connection open JDBC connection (not closed by this method)
     */
    public List<TableInfo> discoverTables(Connection connection) throws SQLException {
        List<String> tableNames =
                options.useTablePattern() ? discoverByPattern(connection) : resolveExplicitList();

        if (tableNames.isEmpty()) {
            throw new IllegalStateException(
                    "No tables found matching configuration: tableList="
                            + options.getTableList()
                            + ", tablePattern="
                            + options.getTablePattern());
        }
        LOG.info("Discovered {} tables: {}", tableNames.size(), tableNames);

        List<TableInfo> result = new ArrayList<>(tableNames.size());
        for (String fullName : tableNames) {
            TableInfo info = buildTableInfo(connection, fullName);
            result.add(info);
            LOG.debug("Resolved table info: {}", info);
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Table name resolution
    // -------------------------------------------------------------------------

    /** Queries {@code information_schema.TABLES} and filters by the configured regex. */
    private List<String> discoverByPattern(Connection connection) throws SQLException {
        Pattern pattern = Pattern.compile(options.getTablePattern());
        String sql = dialect.buildTableListQuery();
        LOG.debug("Discovering tables with query: {}", sql);

        List<String> matched = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, options.getDatabase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String fullName = options.getDatabase() + "." + tableName;
                    if (pattern.matcher(fullName).matches()) {
                        matched.add(fullName);
                        LOG.trace("Regex matched table: {}", fullName);
                    } else {
                        LOG.trace("Regex did not match table: {}", fullName);
                    }
                }
            }
        }
        return matched;
    }

    /** Normalises the explicit table list to fully-qualified "database.tableName" format. */
    private List<String> resolveExplicitList() {
        List<String> result = new ArrayList<>();
        for (String entry : options.getTableList()) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            if (trimmed.contains(".")) {
                result.add(trimmed);
            } else {
                result.add(options.getDatabase() + "." + trimmed);
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // TableInfo construction
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link TableInfo} for the given fully-qualified table name, resolving the split key
     * column.
     */
    private TableInfo buildTableInfo(Connection connection, String fullTableName)
            throws SQLException {
        int dot = fullTableName.indexOf('.');
        String db = dot < 0 ? options.getDatabase() : fullTableName.substring(0, dot);
        String tbl = dot < 0 ? fullTableName : fullTableName.substring(dot + 1);

        String splitKeyColumn;
        int splitKeyJdbcType;

        if (options.getSplitKeyColumn() != null) {
            splitKeyColumn = options.getSplitKeyColumn();
            splitKeyJdbcType = resolveSplitKeyJdbcType(connection, db, tbl, splitKeyColumn);
        } else {
            String pk = discoverPrimaryKey(connection, db, tbl);
            if (pk != null) {
                splitKeyColumn = pk;
                splitKeyJdbcType = resolveSplitKeyJdbcType(connection, db, tbl, splitKeyColumn);
                LOG.debug("Auto-detected primary key '{}' for table {}", pk, fullTableName);
            } else {
                // Fallback: use the first column
                splitKeyColumn = discoverFirstColumn(connection, db, tbl);
                splitKeyJdbcType = resolveSplitKeyJdbcType(connection, db, tbl, splitKeyColumn);
                LOG.warn(
                        "No primary key found for table {}; using first column '{}' as split key. "
                                + "Consider setting splitKeyColumn explicitly for better performance.",
                        fullTableName,
                        splitKeyColumn);
            }
        }

        return new TableInfo(db, tbl, splitKeyColumn, splitKeyJdbcType);
    }

    /** Returns the first PK column name, or {@code null} if no PK exists. */
    private String discoverPrimaryKey(Connection connection, String database, String tableName)
            throws SQLException {
        String sql = dialect.buildPrimaryKeyQuery(database, tableName);
        try (PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /** Returns the first column name of the table (alphabetically first by ordinal position). */
    private String discoverFirstColumn(Connection connection, String database, String tableName)
            throws SQLException {
        try (ResultSet rs = connection.getMetaData().getColumns(database, null, tableName, null)) {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
        }
        throw new SQLException("Table " + database + "." + tableName + " has no columns");
    }

    /** Returns the {@link java.sql.Types} constant for the given column. */
    private int resolveSplitKeyJdbcType(
            Connection connection, String database, String tableName, String columnName)
            throws SQLException {
        try (ResultSet rs =
                connection.getMetaData().getColumns(database, null, tableName, columnName)) {
            if (rs.next()) {
                return rs.getInt("DATA_TYPE");
            }
        }
        // Fall back to VARCHAR if metadata is unavailable.
        LOG.warn(
                "Could not resolve JDBC type for {}.{}.{}; defaulting to VARCHAR",
                database,
                tableName,
                columnName);
        return java.sql.Types.VARCHAR;
    }
}
