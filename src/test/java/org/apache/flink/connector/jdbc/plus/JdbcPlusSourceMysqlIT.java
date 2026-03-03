package org.apache.flink.connector.jdbc.plus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.jdbc.plus.dialect.MySqlDialect;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplitSerializer;
import org.apache.flink.connector.jdbc.plus.splitter.NonUniformChunkSplitter;
import org.apache.flink.connector.jdbc.plus.table.TableDiscovery;
import org.apache.flink.connector.jdbc.plus.table.TableInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Full integration tests for {@link JdbcPlusSource} against a real MySQL instance.
 *
 * <h2>Prerequisites</h2>
 *
 * <p>A running MySQL server with the following configuration:
 *
 * <pre>
 *   Host:     localhost
 *   Port:     3306
 *   Database: flink_jdbc_plus_test
 *   Username: root
 *   Password: root123
 * </pre>
 *
 * Modify {@link #JDBC_URL}, {@link #USERNAME}, and {@link #PASSWORD} to match your environment
 * before running.
 *
 * <h2>What is tested</h2>
 *
 * <ol>
 *   <li>Table discovery via explicit list
 *   <li>Table discovery via regex pattern
 *   <li>Non-uniform chunk splitting (sparse IDs)
 *   <li>DataStream API: read single table, verify row count and field values
 *   <li>DataStream API: read multiple tables (table list)
 *   <li>DataStream API: read multiple tables (regex pattern)
 *   <li>Checkpoint serialization round-trip for splits and enumerator state
 *   <li>Table / SQL API integration
 * </ol>
 *
 * <p>Tests are tagged {@code @Tag("integration")} so they can be excluded from regular CI runs that
 * have no MySQL server:
 *
 * <pre>
 *   mvn test -Dgroups=integration          # run only integration tests
 *   mvn test -DexcludedGroups=integration  # skip integration tests
 * </pre>
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.DisplayName.class)
class JdbcPlusSourceMysqlIT {

    // ── ⚙️  Modify these to match your environment ────────────────────────────
    static final String JDBC_URL =
            "jdbc:mysql://localhost:3306/flink_jdbc_plus_test"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    static final String USERNAME = "root";
    static final String PASSWORD = "root123";
    static final String DATABASE = "flink_jdbc_plus_test";
    // ──────────────────────────────────────────────────────────────────────────

    /** Orders table: id BIGINT PK, amount DECIMAL(10,2), status VARCHAR(32). */
    static final String TABLE_ORDERS = "orders";
    /** Archived orders — identical schema to orders. */
    static final String TABLE_ARCHIVE = "orders_archive";

    private static Connection conn;

    // ── RowType matching both tables (id, amount, status) ─────────────────────
    static final RowType ROW_TYPE =
            RowType.of(new BigIntType(), new DecimalType(10, 2), new VarCharType(32));

    // -------------------------------------------------------------------------
    // Setup / teardown
    // -------------------------------------------------------------------------

    @BeforeAll
    static void createTablesAndInsertData() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Properties props = new Properties();
        props.setProperty("user", USERNAME);
        props.setProperty("password", PASSWORD);
        conn = DriverManager.getConnection(JDBC_URL, props);

        try (Statement st = conn.createStatement()) {
            // Create tables
            st.execute("DROP TABLE IF EXISTS " + TABLE_ORDERS);
            st.execute("DROP TABLE IF EXISTS " + TABLE_ARCHIVE);

            st.execute(
                    "CREATE TABLE "
                            + TABLE_ORDERS
                            + " ("
                            + "  id     BIGINT       NOT NULL AUTO_INCREMENT,"
                            + "  amount DECIMAL(10,2) NOT NULL,"
                            + "  status VARCHAR(32)   NOT NULL,"
                            + "  PRIMARY KEY (id)"
                            + ") ENGINE=InnoDB");

            st.execute(
                    "CREATE TABLE "
                            + TABLE_ARCHIVE
                            + " ("
                            + "  id     BIGINT       NOT NULL,"
                            + "  amount DECIMAL(10,2) NOT NULL,"
                            + "  status VARCHAR(32)   NOT NULL,"
                            + "  PRIMARY KEY (id)"
                            + ") ENGINE=InnoDB");
        }

        // Insert uniform data into orders: 1..100
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "INSERT INTO " + TABLE_ORDERS + " (amount, status) VALUES (?, ?)")) {
            for (int i = 1; i <= 100; i++) {
                ps.setBigDecimal(1, new java.math.BigDecimal(i + ".99"));
                ps.setString(2, i % 2 == 0 ? "PAID" : "PENDING");
                ps.addBatch();
            }
            ps.executeBatch();
        }

        // Insert non-uniform data into orders_archive:
        //   ids: 1..10 (dense), then 1_000_001..1_000_050 (large gap)
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "INSERT INTO "
                                + TABLE_ARCHIVE
                                + " (id, amount, status) VALUES (?, ?, ?)")) {
            for (long id : buildNonUniformIds()) {
                ps.setLong(1, id);
                ps.setBigDecimal(2, new java.math.BigDecimal("9.99"));
                ps.setString(3, "ARCHIVED");
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    @AfterAll
    static void dropTables() throws Exception {
        if (conn != null && !conn.isClosed()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + TABLE_ORDERS);
                st.execute("DROP TABLE IF EXISTS " + TABLE_ARCHIVE);
            }
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // 1. Table discovery — explicit list
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("1. TableDiscovery: explicit list resolves both tables")
    void tableDiscovery_explicitList() throws Exception {
        JdbcPlusOptions options = baseOptions().tableList(TABLE_ORDERS, TABLE_ARCHIVE).build();

        TableDiscovery discovery = new TableDiscovery(options, MySqlDialect.INSTANCE);
        List<TableInfo> tables = discovery.discoverTables(conn);

        assertEquals(2, tables.size(), "Should discover both tables");

        List<String> names = new ArrayList<>();
        tables.forEach(t -> names.add(t.getTableName()));
        assertTrue(names.contains(TABLE_ORDERS));
        assertTrue(names.contains(TABLE_ARCHIVE));
    }

    // -------------------------------------------------------------------------
    // 2. Table discovery — regex pattern
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("2. TableDiscovery: regex pattern matches both order tables")
    void tableDiscovery_regex() throws Exception {
        // Pattern matches both "orders" and "orders_archive"
        JdbcPlusOptions options = baseOptions().tablePattern(DATABASE + "\\.orders.*").build();

        TableDiscovery discovery = new TableDiscovery(options, MySqlDialect.INSTANCE);
        List<TableInfo> tables = discovery.discoverTables(conn);

        assertEquals(2, tables.size(), "Regex should match both order tables");
        assertTrue(tables.stream().anyMatch(t -> t.getTableName().equals(TABLE_ORDERS)));
        assertTrue(tables.stream().anyMatch(t -> t.getTableName().equals(TABLE_ARCHIVE)));
    }

    @Test
    @DisplayName("3. TableDiscovery: narrower regex matches only one table")
    void tableDiscovery_narrowRegex() throws Exception {
        JdbcPlusOptions options =
                baseOptions().tablePattern(DATABASE + "\\." + TABLE_ARCHIVE).build();

        TableDiscovery discovery = new TableDiscovery(options, MySqlDialect.INSTANCE);
        List<TableInfo> tables = discovery.discoverTables(conn);

        assertEquals(1, tables.size());
        assertEquals(TABLE_ARCHIVE, tables.get(0).getTableName());
    }

    // -------------------------------------------------------------------------
    // 3. Non-uniform chunk splitting
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("4. NonUniformSplitter: orders (uniform 100 rows, chunkSize=20) → 5 splits")
    void nonUniformSplitter_uniformData() throws Exception {
        TableInfo tableInfo = new TableInfo(DATABASE, TABLE_ORDERS, "id", java.sql.Types.BIGINT);
        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(MySqlDialect.INSTANCE, 20);

        List<JdbcSourceSplit> splits = splitter.split(conn, tableInfo);

        // 100 rows / 20 per chunk = 5 chunks
        assertEquals(5, splits.size(), "Should produce 5 splits for 100 rows, chunkSize=20");

        // First split: no start, end=20
        assertNull((Object) splits.get(0).getSplitStart());
        assertEquals(20L, splits.get(0).getSplitEnd());

        // Last split: start=80, no end
        assertEquals(80L, splits.get(4).getSplitStart());
        assertNull((Object) splits.get(4).getSplitEnd());
    }

    @Test
    @DisplayName(
            "5. NonUniformSplitter: orders_archive (non-uniform IDs, chunkSize=10) → correct splits")
    void nonUniformSplitter_nonUniformData() throws Exception {
        // orders_archive has ids: 1..10 (dense) + 1_000_001..1_000_050 (gap of ~999_990)
        // Total: 60 rows, chunkSize=10 → 6 splits
        TableInfo tableInfo = new TableInfo(DATABASE, TABLE_ARCHIVE, "id", java.sql.Types.BIGINT);
        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(MySqlDialect.INSTANCE, 10);

        List<JdbcSourceSplit> splits = splitter.split(conn, tableInfo);

        assertEquals(6, splits.size(), "60 rows with chunkSize=10 should produce 6 splits");

        // With arithmetic splitting the first chunk would be [1, ~166_668] — containing all rows.
        // With data-driven splitting the first chunk ends at id=10 (the 10th row).
        assertEquals(
                10L,
                splits.get(0).getSplitEnd(),
                "First split must end at the actual 10th row (id=10), not at an arithmetic boundary");

        // Second split covers first 10 rows of the dense block in the gap area
        assertEquals(10L, splits.get(1).getSplitStart());
        assertEquals(1_000_010L, splits.get(1).getSplitEnd());

        assertNull((Object) splits.get(5).getSplitEnd(), "Last split should be unbounded");
    }

    // -------------------------------------------------------------------------
    // 4. Checkpoint serialization
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("6. Checkpoint: split serialization round-trip")
    void splitSerializerRoundTrip() throws Exception {
        JdbcSourceSplit original =
                new JdbcSourceSplit(
                        "flink_jdbc_plus_test.orders#2",
                        DATABASE + ".orders",
                        "id",
                        1000L,
                        2000L,
                        42L);

        JdbcSourceSplitSerializer serializer = new JdbcSourceSplitSerializer();
        byte[] bytes = serializer.serialize(original);
        JdbcSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(original.splitId(), restored.splitId());
        assertEquals(original.getFullTableName(), restored.getFullTableName());
        assertEquals(original.getSplitKeyColumn(), restored.getSplitKeyColumn());
        assertEquals(original.getSplitStart(), restored.getSplitStart());
        assertEquals(original.getSplitEnd(), restored.getSplitEnd());
        assertEquals(original.getOffset(), restored.getOffset());
    }

    // -------------------------------------------------------------------------
    // 5. DataStream API — single table
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("7. DataStream: read single table (orders) → 100 rows")
    void dataStream_singleTable() throws Exception {
        JdbcPlusSource source =
                JdbcPlusSource.builder()
                        .url(JDBC_URL)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .database(DATABASE)
                        .tableList(TABLE_ORDERS)
                        .chunkSize(20)
                        .fetchSize(50)
                        .rowType(ROW_TYPE)
                        .build();

        List<RowData> rows = collectFromSource(source, "single-table-test");

        assertEquals(100, rows.size(), "Should read all 100 rows from orders");

        // Spot-check: all status values should be 'PAID' or 'PENDING'
        for (RowData row : rows) {
            String status = row.getString(2).toString();
            assertTrue(
                    status.equals("PAID") || status.equals("PENDING"),
                    "Unexpected status: " + status);
        }
    }

    // -------------------------------------------------------------------------
    // 6. DataStream API — multiple tables (explicit list)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("8. DataStream: read multiple tables (list) → 100 + 60 = 160 rows")
    void dataStream_multiTable_list() throws Exception {
        JdbcPlusSource source =
                JdbcPlusSource.builder()
                        .url(JDBC_URL)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .database(DATABASE)
                        .tableList(TABLE_ORDERS, TABLE_ARCHIVE)
                        .chunkSize(20)
                        .fetchSize(50)
                        .rowType(ROW_TYPE)
                        .build();

        List<RowData> rows = collectFromSource(source, "multi-table-list-test");

        assertEquals(
                160, rows.size(), "Should read 100 rows from orders + 60 rows from orders_archive");
    }

    // -------------------------------------------------------------------------
    // 7. DataStream API — multiple tables (regex)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("9. DataStream: read multiple tables (regex) → 100 + 60 = 160 rows")
    void dataStream_multiTable_regex() throws Exception {
        JdbcPlusSource source =
                JdbcPlusSource.builder()
                        .url(JDBC_URL)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .database(DATABASE)
                        .tablePattern(DATABASE + "\\.orders.*")
                        .chunkSize(20)
                        .fetchSize(50)
                        .rowType(ROW_TYPE)
                        .build();

        List<RowData> rows = collectFromSource(source, "multi-table-regex-test");

        assertEquals(160, rows.size(), "Regex pattern should pick up both tables → 160 rows total");
    }

    // -------------------------------------------------------------------------
    // 8. DataStream API — non-uniform data, verify all rows arrived
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("10. DataStream: non-uniform archive table → all 60 rows read (no data loss)")
    void dataStream_nonUniformArchive() throws Exception {
        JdbcPlusSource source =
                JdbcPlusSource.builder()
                        .url(JDBC_URL)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .database(DATABASE)
                        .tableList(TABLE_ARCHIVE)
                        .chunkSize(10) // small chunk to exercise multi-split path
                        .fetchSize(20)
                        .rowType(ROW_TYPE)
                        .build();

        List<RowData> rows = collectFromSource(source, "nonuniform-archive-test");

        assertEquals(
                60,
                rows.size(),
                "All 60 rows from orders_archive must be read despite the large ID gap");

        // Verify IDs: must contain all of 1..10 and 1_000_001..1_000_050
        List<Long> ids = new ArrayList<>();
        rows.forEach(r -> ids.add(r.getLong(0)));
        for (long i = 1; i <= 10; i++) {
            assertTrue(ids.contains(i), "Missing id=" + i + " from dense block");
        }
        for (long i = 1_000_001; i <= 1_000_050; i++) {
            assertTrue(ids.contains(i), "Missing id=" + i + " from sparse block");
        }
    }

    // -------------------------------------------------------------------------
    // 9. Table API / SQL
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("11. Table SQL API: COUNT(*) on orders via jdbc-plus connector")
    void tableApi_sqlCount() throws Exception {
        TableEnvironment tenv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        tenv.executeSql(
                "CREATE TABLE orders_src ("
                        + "  id     BIGINT,"
                        + "  amount DECIMAL(10, 2),"
                        + "  status VARCHAR(32)"
                        + ") WITH ("
                        + "  'connector'  = 'jdbc-plus',"
                        + "  'url'        = '"
                        + JDBC_URL
                        + "',"
                        + "  'username'   = '"
                        + USERNAME
                        + "',"
                        + "  'password'   = '"
                        + PASSWORD
                        + "',"
                        + "  'database'   = '"
                        + DATABASE
                        + "',"
                        + "  'table-list' = '"
                        + TABLE_ORDERS
                        + "',"
                        + "  'chunk-size' = '25'"
                        + ")");

        TableResult result = tenv.executeSql("SELECT COUNT(*) FROM orders_src");
        Iterator<?> iter = result.collect();
        assertTrue(iter.hasNext(), "COUNT(*) should return a row");
        Object row = iter.next();
        assertNotNull(row);
        // The single result cell should be 100
        assertEquals(100L, ((org.apache.flink.types.Row) row).getField(0));
        assertFalse(iter.hasNext(), "COUNT(*) should return exactly one row");
    }

    @Test
    @DisplayName("12. Table SQL API: read multi-table via regex pattern")
    void tableApi_sqlMultiTableRegex() throws Exception {
        TableEnvironment tenv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        tenv.executeSql(
                "CREATE TABLE all_orders ("
                        + "  id     BIGINT,"
                        + "  amount DECIMAL(10, 2),"
                        + "  status VARCHAR(32)"
                        + ") WITH ("
                        + "  'connector'     = 'jdbc-plus',"
                        + "  'url'           = '"
                        + JDBC_URL
                        + "',"
                        + "  'username'      = '"
                        + USERNAME
                        + "',"
                        + "  'password'      = '"
                        + PASSWORD
                        + "',"
                        + "  'database'      = '"
                        + DATABASE
                        + "',"
                        + "  'table-pattern' = '"
                        + DATABASE
                        + "\\\\.orders.*',"
                        + "  'chunk-size'    = '20'"
                        + ")");

        TableResult result = tenv.executeSql("SELECT COUNT(*) FROM all_orders");
        Iterator<?> iter = result.collect();
        assertTrue(iter.hasNext());
        assertEquals(
                160L,
                ((org.apache.flink.types.Row) iter.next()).getField(0),
                "Regex should match both tables → 160 total rows");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Runs a Flink job locally with the given {@link JdbcPlusSource} and collects all output rows
     * into a list.
     */
    private static List<RowData> collectFromSource(JdbcPlusSource source, String jobName)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setRestartStrategy(RestartStrategies.noRestart());

        List<RowData> collected = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<RowData> iter =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), jobName)
                        .executeAndCollect(jobName)) {
            iter.forEachRemaining(collected::add);
        }
        return collected;
    }

    /** Builds a {@link JdbcPlusOptions.Builder} pre-filled with connection settings. */
    private static JdbcPlusOptions.Builder baseOptions() {
        return JdbcPlusOptions.builder()
                .url(JDBC_URL)
                .username(USERNAME)
                .password(PASSWORD)
                .database(DATABASE);
    }

    /** Builds non-uniform IDs: 1..10 dense, then 1_000_001..1_000_050 (large gap). */
    private static long[] buildNonUniformIds() {
        long[] ids = new long[60];
        for (int i = 0; i < 10; i++) ids[i] = i + 1;
        for (int i = 0; i < 50; i++) ids[10 + i] = 1_000_001L + i;
        return ids;
    }
}
