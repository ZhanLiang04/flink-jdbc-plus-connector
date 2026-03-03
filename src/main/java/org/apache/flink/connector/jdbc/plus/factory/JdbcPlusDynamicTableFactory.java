package org.apache.flink.connector.jdbc.plus.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.jdbc.plus.JdbcPlusOptions;
import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.plus.dialect.MySqlDialect;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Flink Table API factory for the {@code jdbc-plus} connector.
 *
 * <h2>DDL example – explicit table list</h2>
 *
 * <pre>{@code
 * CREATE TABLE orders_source (
 *   id       BIGINT,
 *   amount   DECIMAL(10, 2),
 *   status   STRING
 * ) WITH (
 *   'connector'   = 'jdbc-plus',
 *   'url'         = 'jdbc:mysql://localhost:3306/mydb',
 *   'username'    = 'root',
 *   'password'    = 'secret',
 *   'database'    = 'mydb',
 *   'table-list'  = 'orders,orders_archive',
 *   'chunk-size'  = '8096'
 * );
 * }</pre>
 *
 * <h2>DDL example – regex pattern</h2>
 *
 * <pre>{@code
 * CREATE TABLE all_orders (
 *   ...
 * ) WITH (
 *   'connector'     = 'jdbc-plus',
 *   'url'           = 'jdbc:mysql://localhost:3306/mydb',
 *   'database'      = 'mydb',
 *   'table-pattern' = 'mydb\\.order_.*',
 *   'chunk-size'    = '4096'
 * );
 * }</pre>
 */
public class JdbcPlusDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "jdbc-plus";

    // ── Required options ────────────────────────────────────────────────────

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JDBC connection URL.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Default database/schema name.");

    // ── Optional: table selection ───────────────────────────────────────────

    public static final ConfigOption<String> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated list of table names (within 'database'), "
                                    + "or fully-qualified 'db.table' entries. "
                                    + "Mutually exclusive with 'table-pattern'.");

    public static final ConfigOption<String> TABLE_PATTERN =
            ConfigOptions.key("table-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Java regex matched against 'database.tableName' for dynamic "
                                    + "table discovery. Mutually exclusive with 'table-list'.");

    // ── Optional: connection ────────────────────────────────────────────────

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JDBC username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JDBC password.");

    public static final ConfigOption<String> DRIVER =
            ConfigOptions.key("driver")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JDBC driver class name. Auto-detected from URL scheme when not set.");

    // ── Optional: splitting ─────────────────────────────────────────────────

    public static final ConfigOption<String> SPLIT_KEY_COLUMN =
            ConfigOptions.key("split-key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Column used for non-uniform chunk splitting. "
                                    + "Defaults to the table's first primary key column.");

    public static final ConfigOption<Integer> CHUNK_SIZE =
            ConfigOptions.key("chunk-size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription("Target number of rows per chunk. Default: 8096.");

    public static final ConfigOption<Integer> FETCH_SIZE =
            ConfigOptions.key("fetch-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("JDBC fetch size per round-trip. Default: 1024.");

    // ── Optional: reliability ───────────────────────────────────────────────

    public static final ConfigOption<Integer> CONNECTION_TIMEOUT_MS =
            ConfigOptions.key("connection-timeout-ms")
                    .intType()
                    .defaultValue(30_000)
                    .withDescription("JDBC connection timeout in milliseconds. Default: 30000.");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Number of retries on transient errors. Default: 3.");

    // ── Optional: dialect ───────────────────────────────────────────────────

    public static final ConfigOption<String> DIALECT =
            ConfigOptions.key("dialect")
                    .stringType()
                    .defaultValue("mysql")
                    .withDescription("SQL dialect: 'mysql' (default). Extend for other databases.");

    // -------------------------------------------------------------------------
    // Factory interface
    // -------------------------------------------------------------------------

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Arrays.asList(URL, DATABASE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        TABLE_LIST,
                        TABLE_PATTERN,
                        USERNAME,
                        PASSWORD,
                        DRIVER,
                        SPLIT_KEY_COLUMN,
                        CHUNK_SIZE,
                        FETCH_SIZE,
                        CONNECTION_TIMEOUT_MS,
                        MAX_RETRIES,
                        DIALECT));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        // ── Build options ──────────────────────────────────────────────────

        String tableListRaw = helper.getOptions().getOptional(TABLE_LIST).orElse(null);
        String tablePattern = helper.getOptions().getOptional(TABLE_PATTERN).orElse(null);

        JdbcPlusOptions.Builder optBuilder =
                JdbcPlusOptions.builder()
                        .url(helper.getOptions().get(URL))
                        .database(helper.getOptions().get(DATABASE))
                        .chunkSize(helper.getOptions().get(CHUNK_SIZE))
                        .fetchSize(helper.getOptions().get(FETCH_SIZE))
                        .connectionTimeoutMs(helper.getOptions().get(CONNECTION_TIMEOUT_MS))
                        .maxRetries(helper.getOptions().get(MAX_RETRIES));

        helper.getOptions().getOptional(USERNAME).ifPresent(optBuilder::username);
        helper.getOptions().getOptional(PASSWORD).ifPresent(optBuilder::password);
        helper.getOptions().getOptional(DRIVER).ifPresent(optBuilder::driverName);
        helper.getOptions().getOptional(SPLIT_KEY_COLUMN).ifPresent(optBuilder::splitKeyColumn);

        if (tableListRaw != null) {
            List<String> tables =
                    Arrays.stream(tableListRaw.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
            optBuilder.tableList(tables);
        } else if (tablePattern != null) {
            optBuilder.tablePattern(tablePattern);
        } else {
            throw new IllegalArgumentException(
                    "jdbc-plus requires either 'table-list' or 'table-pattern' to be set.");
        }

        JdbcPlusOptions options = optBuilder.build();

        // ── Dialect ────────────────────────────────────────────────────────

        JdbcDialect dialect = resolveDialect(helper.getOptions().get(DIALECT));

        // ── Row type from the DDL schema ───────────────────────────────────

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();

        return new JdbcPlusDynamicTableSource(options, dialect, rowType);
    }

    // -------------------------------------------------------------------------

    private JdbcDialect resolveDialect(String dialectName) {
        switch (dialectName.toLowerCase()) {
            case "mysql":
            default:
                return MySqlDialect.INSTANCE;
        }
    }
}
