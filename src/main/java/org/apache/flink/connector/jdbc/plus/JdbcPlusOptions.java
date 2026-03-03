package org.apache.flink.connector.jdbc.plus;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable configuration options for the Flink JDBC Plus connector.
 *
 * <p>Supports multi-table specification via an explicit list or a regex pattern, configurable
 * non-uniform chunk splitting, and JDBC connection settings.
 */
public class JdbcPlusOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /** JDBC connection URL, e.g. "jdbc:mysql://localhost:3306/mydb". */
    private final String url;

    private final String username;
    private final String password;

    /**
     * Optional explicit driver class name. When null the driver is auto-detected from the URL
     * scheme (mysql → com.mysql.cj.jdbc.Driver, etc.).
     */
    private final String driverName;

    /** Default database/schema name, used for table discovery and SQL generation. */
    private final String database;

    /**
     * Explicit list of tables to read. Each entry is "tableName" (within {@link #database}) or
     * "database.tableName". Mutually exclusive with {@link #tablePattern}.
     */
    private final List<String> tableList;

    /**
     * Java regex matching table names (against "database.tableName") for dynamic discovery.
     * Mutually exclusive with {@link #tableList}.
     */
    private final String tablePattern;

    /**
     * Column used to partition each table into chunks. When null the connector auto-detects the
     * primary key column.
     */
    private final String splitKeyColumn;

    /**
     * Target number of rows per chunk. The non-uniform splitter queries actual data boundaries so
     * that each chunk contains approximately this many rows. Default: 8096.
     */
    private final int chunkSize;

    /** Fetch size hint passed to the JDBC driver for each chunk query. Default: 1024. */
    private final int fetchSize;

    /** Connection timeout in milliseconds. Default: 30 000. */
    private final int connectionTimeoutMs;

    /** Number of retries on transient JDBC errors. Default: 3. */
    private final int maxRetries;

    private JdbcPlusOptions(Builder builder) {
        this.url = Objects.requireNonNull(builder.url, "url must not be null");
        this.username = builder.username;
        this.password = builder.password;
        this.driverName = builder.driverName;
        this.database = Objects.requireNonNull(builder.database, "database must not be null");
        this.tableList =
                builder.tableList == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(new ArrayList<>(builder.tableList));
        this.tablePattern = builder.tablePattern;
        this.splitKeyColumn = builder.splitKeyColumn;
        this.chunkSize = builder.chunkSize;
        this.fetchSize = builder.fetchSize;
        this.connectionTimeoutMs = builder.connectionTimeoutMs;
        this.maxRetries = builder.maxRetries;

        if (tableList.isEmpty() && (tablePattern == null || tablePattern.isEmpty())) {
            throw new IllegalArgumentException(
                    "Either tableList or tablePattern must be specified");
        }
        if (!tableList.isEmpty() && tablePattern != null && !tablePattern.isEmpty()) {
            throw new IllegalArgumentException("tableList and tablePattern are mutually exclusive");
        }
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public List<String> getTableList() {
        return tableList;
    }

    public String getTablePattern() {
        return tablePattern;
    }

    public String getSplitKeyColumn() {
        return splitKeyColumn;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public boolean useTablePattern() {
        return tablePattern != null && !tablePattern.isEmpty();
    }

    /**
     * Resolves the JDBC driver class name: uses {@link #driverName} if set, otherwise tries to
     * infer from the URL scheme.
     */
    public String resolveDriverName() {
        if (driverName != null) {
            return driverName;
        }
        if (url.startsWith("jdbc:mysql")) {
            return "com.mysql.cj.jdbc.Driver";
        }
        if (url.startsWith("jdbc:postgresql")) {
            return "org.postgresql.Driver";
        }
        if (url.startsWith("jdbc:oracle")) {
            return "oracle.jdbc.OracleDriver";
        }
        if (url.startsWith("jdbc:sqlserver")) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
        throw new IllegalStateException(
                "Cannot infer JDBC driver from URL: "
                        + url
                        + ". Please set driverName explicitly.");
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String url;
        private String username;
        private String password;
        private String driverName;
        private String database;
        private List<String> tableList;
        private String tablePattern;
        private String splitKeyColumn;
        private int chunkSize = 8096;
        private int fetchSize = 1024;
        private int connectionTimeoutMs = 30_000;
        private int maxRetries = 3;

        private Builder() {}

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder driverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        /** Explicit table list, e.g. {@code "orders", "customers", "products"}. */
        public Builder tableList(List<String> tableList) {
            this.tableList = tableList;
            return this;
        }

        public Builder tableList(String... tables) {
            this.tableList = Arrays.asList(tables);
            return this;
        }

        /**
         * Java regex matched against fully-qualified "database.tableName", e.g. {@code
         * "mydb\\.order_.*"} to discover all order-related tables.
         */
        public Builder tablePattern(String tablePattern) {
            this.tablePattern = tablePattern;
            return this;
        }

        /** Override the split key column (defaults to the table's first primary key column). */
        public Builder splitKeyColumn(String splitKeyColumn) {
            this.splitKeyColumn = splitKeyColumn;
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public Builder connectionTimeoutMs(int connectionTimeoutMs) {
            this.connectionTimeoutMs = connectionTimeoutMs;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public JdbcPlusOptions build() {
            return new JdbcPlusOptions(this);
        }
    }
}
