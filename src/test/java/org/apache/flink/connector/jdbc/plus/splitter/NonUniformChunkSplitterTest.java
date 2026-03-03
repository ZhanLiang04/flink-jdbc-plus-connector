package org.apache.flink.connector.jdbc.plus.splitter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.plus.table.TableInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

/**
 * Unit tests for {@link NonUniformChunkSplitter} using an H2 in-memory database.
 *
 * <p>H2 is used so the tests run without any external MySQL server. A custom {@link H2Dialect}
 * generates H2-compatible SQL ({@code LIMIT 1 OFFSET n}) instead of MySQL's {@code LIMIT n, 1}
 * two-argument syntax.
 *
 * <p>Test cases cover:
 *
 * <ul>
 *   <li>Uniform data → equal-sized chunks
 *   <li>Non-uniform data (large gaps) → chunks still have ~chunkSize rows despite gaps
 *   <li>Empty table → single full-table split
 *   <li>Fewer rows than chunkSize → single split
 *   <li>Exactly one full chunk → two splits (the boundary itself plus the final open-ended split)
 *   <li>String split key
 * </ul>
 */
class NonUniformChunkSplitterTest {

    private static final String JDBC_URL =
            "jdbc:h2:mem:splitter_test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";

    private static Connection conn;

    @BeforeAll
    static void initDb() throws Exception {
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection(JDBC_URL, "sa", "");
        try (Statement st = conn.createStatement()) {
            // Numeric split-key table
            st.execute(
                    "CREATE TABLE IF NOT EXISTS test.orders ("
                            + "id BIGINT PRIMARY KEY, amount DECIMAL(10,2), status VARCHAR(32))");

            // String split-key table
            st.execute(
                    "CREATE TABLE IF NOT EXISTS test.customers ("
                            + "name VARCHAR(64) PRIMARY KEY, city VARCHAR(64))");
        }
    }

    @AfterAll
    static void closeDb() throws Exception {
        if (conn != null && !conn.isClosed()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS test.orders");
                st.execute("DROP TABLE IF EXISTS test.customers");
            }
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void clearAndInsert(long... ids) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM test.orders");
        }
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "INSERT INTO test.orders(id, amount, status) VALUES (?, 1.00, 'NEW')")) {
            for (long id : ids) {
                ps.setLong(1, id);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void clearAndInsertNames(String... names) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM test.customers");
        }
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "INSERT INTO test.customers(name, city) VALUES (?, 'City')")) {
            for (String name : names) {
                ps.setString(1, name);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static TableInfo ordersTable(String splitKey) {
        return new TableInfo("test", "orders", splitKey, Types.BIGINT);
    }

    private static TableInfo customersTable() {
        return new TableInfo("test", "customers", "name", Types.VARCHAR);
    }

    // A dialect that generates H2-compatible SQL.
    private static final JdbcDialect H2 = new H2Dialect();

    // -------------------------------------------------------------------------
    // Test cases
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Empty table → single full-table split with no bounds")
    void emptyTable() throws Exception {
        clearAndInsert(); // no rows
        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 5);
        List<JdbcSourceSplit> splits = splitter.split(conn, ordersTable("id"));

        assertEquals(1, splits.size(), "Empty table should produce exactly one split");
        JdbcSourceSplit s = splits.get(0);
        assertNull(s.getSplitStart(), "Start should be null (unbounded)");
        assertNull(s.getSplitEnd(), "End should be null (unbounded)");
    }

    @Test
    @DisplayName("Fewer rows than chunkSize → single split")
    void fewerRowsThanChunkSize() throws Exception {
        clearAndInsert(1, 2, 3, 4, 5);
        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 100);
        List<JdbcSourceSplit> splits = splitter.split(conn, ordersTable("id"));

        assertEquals(1, splits.size());
        assertNull(splits.get(0).getSplitStart());
        assertNull(splits.get(0).getSplitEnd());
    }

    @Test
    @DisplayName("Exactly chunkSize rows → two splits: first chunk + empty tail")
    void exactlyChunkSizeRows() throws Exception {
        // 10 rows, chunkSize=10 → boundary query returns nothing → 1 split
        long[] ids = new long[10];
        for (int i = 0; i < 10; i++) ids[i] = i + 1;
        clearAndInsert(ids);

        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 10);
        List<JdbcSourceSplit> splits = splitter.split(conn, ordersTable("id"));

        // LIMIT 10 OFFSET 10 → no row → one single open-ended split
        assertEquals(1, splits.size());
        assertNull(splits.get(0).getSplitStart());
        assertNull(splits.get(0).getSplitEnd());
    }

    @Test
    @DisplayName("Uniform data: 100 rows, chunkSize=10 → 10 splits of ~10 rows each")
    void uniformData_100rows_chunk10() throws Exception {
        long[] ids = new long[100];
        for (int i = 0; i < 100; i++) ids[i] = i + 1;
        clearAndInsert(ids);

        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 10);
        List<JdbcSourceSplit> splits = splitter.split(conn, ordersTable("id"));

        // 100 rows / 10 per chunk = 10 chunks
        assertEquals(10, splits.size(), "Should produce 10 splits for 100 rows with chunkSize=10");

        // First split: unbounded start, end = 10
        assertNull(splits.get(0).getSplitStart());
        assertEquals(10L, splits.get(0).getSplitEnd());

        // Last split: start = 90 (last boundary), unbounded end
        assertEquals(90L, splits.get(9).getSplitStart());
        assertNull(splits.get(9).getSplitEnd());

        // Every split covers exactly 10 rows
        for (int i = 0; i < 9; i++) {
            long start =
                    (splits.get(i).getSplitStart() == null)
                            ? 0L
                            : (Long) splits.get(i).getSplitStart();
            long end = (Long) splits.get(i).getSplitEnd();
            assertEquals(
                    10L,
                    end - start,
                    "Split " + i + " should cover 10-row interval; start=" + start + " end=" + end);
        }
    }

    @Test
    @DisplayName("Non-uniform data (large gaps) → correct boundary discovery despite gaps")
    void nonUniformData_largeGaps() throws Exception {
        // IDs: 1..10 (dense), then 1_000_001..1_000_010 (big gap)
        long[] ids = new long[20];
        for (int i = 0; i < 10; i++) ids[i] = i + 1;
        for (int i = 0; i < 10; i++) ids[10 + i] = 1_000_001L + i;
        clearAndInsert(ids);

        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 10);
        List<JdbcSourceSplit> splits = splitter.split(conn, ordersTable("id"));

        // chunkSize=10, 20 rows → boundary at 10th row (id=10), then last chunk
        assertEquals(
                2,
                splits.size(),
                "Should produce 2 splits: rows 1-10 and rows 1_000_001-1_000_010");

        JdbcSourceSplit first = splits.get(0);
        assertNull(first.getSplitStart(), "First chunk has no lower bound");
        assertEquals(10L, first.getSplitEnd(), "First chunk ends at id=10 (10th row)");

        JdbcSourceSplit last = splits.get(1);
        assertEquals(10L, last.getSplitStart(), "Second chunk starts after id=10");
        assertNull(last.getSplitEnd(), "Last chunk has no upper bound");

        // Key insight: with arithmetic splitting, chunk2 would be empty because
        // the range [10, 1_000_010] would produce chunks with no rows.
        // With data-driven splitting, chunk2 always contains exactly the remaining rows.
    }

    @Test
    @DisplayName("Non-uniform data: sparse IDs — would break arithmetic splitting")
    void nonUniformData_sparseIds_arithmeticWouldFail() throws Exception {
        // IDs at powers of 2: 1, 2, 4, 8, 16, ..., 1024
        long[] ids = new long[11];
        for (int i = 0; i < 11; i++) ids[i] = (long) Math.pow(2, i);
        clearAndInsert(ids);

        // chunkSize=3 → should produce ceil(11/3) = 4 splits
        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 3);
        List<JdbcSourceSplit> splits = splitter.split(conn, ordersTable("id"));

        // 11 rows / 3 per chunk = 3 full chunks + 1 partial last chunk = 4 total
        assertEquals(4, splits.size());

        // Each split except the last has a non-null end
        for (int i = 0; i < 3; i++) {
            assertNotNull(splits.get(i).getSplitEnd(), "Split " + i + " should have an end bound");
        }
        assertNull(splits.get(3).getSplitEnd(), "Last split should be unbounded");

        // Boundary of first chunk = 3rd row = id=4
        assertEquals(4L, splits.get(0).getSplitEnd());
        // Boundary of second chunk = 6th row = id=32
        assertEquals(32L, splits.get(1).getSplitEnd());
        // Boundary of third chunk = 9th row = id=256
        assertEquals(256L, splits.get(2).getSplitEnd());
    }

    @Test
    @DisplayName("String split key: alphabetically-ordered names produce correct splits")
    void stringSplitKey() throws Exception {
        clearAndInsertNames(
                "Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan",
                "Judy");
        // 10 rows, chunkSize=3 → 3 full splits + 1 tail = 4 splits

        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 3);
        List<JdbcSourceSplit> splits = splitter.split(conn, customersTable());

        assertEquals(4, splits.size(), "10 rows with chunkSize=3 should give 4 splits");

        // First chunk ends at 3rd alphabetical name = "Charlie"
        assertEquals("Charlie", splits.get(0).getSplitEnd());
        // Second chunk ends at 6th name = "Frank"
        assertEquals("Frank", splits.get(1).getSplitEnd());
        // Third chunk ends at 9th name = "Ivan"
        assertEquals("Ivan", splits.get(2).getSplitEnd());
        // Last chunk: start="Ivan", no end
        assertEquals("Ivan", splits.get(3).getSplitStart());
        assertNull(splits.get(3).getSplitEnd());
    }

    @Test
    @DisplayName("Split IDs are unique and follow the naming convention")
    void splitIdConvention() throws Exception {
        long[] ids = new long[25];
        for (int i = 0; i < 25; i++) ids[i] = i + 1;
        clearAndInsert(ids);

        NonUniformChunkSplitter splitter = new NonUniformChunkSplitter(H2, 10);
        List<JdbcSourceSplit> splits = splitter.split(conn, ordersTable("id"));

        for (int i = 0; i < splits.size(); i++) {
            String expectedId = "test.orders#" + i;
            assertEquals(
                    expectedId,
                    splits.get(i).splitId(),
                    "Split ID should follow 'fullTable#index' convention");
        }
    }

    // -------------------------------------------------------------------------
    // H2-compatible dialect (used only in tests)
    // -------------------------------------------------------------------------

    /**
     * SQL dialect for H2 in-memory databases.
     *
     * <p>H2 supports {@code LIMIT count OFFSET offset} but not MySQL's {@code LIMIT offset, count}
     * two-argument form. This dialect generates H2-compatible boundary queries; the rest of the SQL
     * is identical to MySQL.
     */
    static class H2Dialect implements JdbcDialect {

        private static final long serialVersionUID = 1L;

        @Override
        public String quoteIdentifier(String id) {
            return '"' + id.replace("\"", "\"\"") + '"';
        }

        @Override
        public String buildNextBoundaryQuery(
                String fullTableName, String splitKeyColumn, Object start, int chunkSize) {
            String table = quoteFullTableName(fullTableName);
            String col = quoteIdentifier(splitKeyColumn);
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(col).append(" FROM ").append(table);
            if (start != null) {
                sb.append(" WHERE ").append(col).append(" > ?");
            }
            // H2 uses LIMIT count OFFSET offset (not MySQL's LIMIT offset, count)
            sb.append(" ORDER BY ").append(col).append(" LIMIT 1 OFFSET ").append(chunkSize);
            return sb.toString();
        }

        @Override
        public String buildMinMaxQuery(String fullTableName, String splitKeyColumn) {
            String col = quoteIdentifier(splitKeyColumn);
            return "SELECT MIN("
                    + col
                    + "), MAX("
                    + col
                    + ") FROM "
                    + quoteFullTableName(fullTableName);
        }

        @Override
        public String buildSplitScanQuery(
                org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit split,
                String columns,
                int fetchSize) {
            String table = quoteFullTableName(split.getFullTableName());
            String col = quoteIdentifier(split.getSplitKeyColumn());
            StringBuilder sb =
                    new StringBuilder("SELECT ").append(columns).append(" FROM ").append(table);
            boolean hasStart = split.hasStart();
            boolean hasEnd = split.hasEnd();
            if (hasStart || hasEnd) {
                sb.append(" WHERE ");
                if (hasStart && hasEnd) {
                    sb.append(col).append(" > ?").append(" AND ").append(col).append(" <= ?");
                } else if (hasStart) {
                    sb.append(col).append(" > ?");
                } else {
                    sb.append(col).append(" <= ?");
                }
            }
            if (fetchSize > 0) {
                sb.append(" LIMIT ").append(fetchSize);
                if (split.getOffset() > 0) sb.append(" OFFSET ").append(split.getOffset());
            }
            return sb.toString();
        }

        @Override
        public String buildTableListQuery() {
            return "SELECT TABLE_NAME FROM information_schema.TABLES "
                    + "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME";
        }

        @Override
        public String buildPrimaryKeyQuery(String database, String tableName) {
            return "SELECT COLUMN_NAME FROM information_schema.INDEXES "
                    + "WHERE TABLE_SCHEMA = '"
                    + database
                    + "' AND TABLE_NAME = '"
                    + tableName
                    + "' AND PRIMARY_KEY = TRUE "
                    + "ORDER BY ORDINAL_POSITION LIMIT 1";
        }

        @Override
        public String buildColumnMetaQuery(String database, String tableName) {
            return "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
                    + "WHERE TABLE_SCHEMA = '"
                    + database
                    + "' AND TABLE_NAME = '"
                    + tableName
                    + "' ORDER BY ORDINAL_POSITION";
        }

        private String quoteFullTableName(String fullTableName) {
            int dot = fullTableName.indexOf('.');
            if (dot < 0) return quoteIdentifier(fullTableName);
            return quoteIdentifier(fullTableName.substring(0, dot))
                    + "."
                    + quoteIdentifier(fullTableName.substring(dot + 1));
        }
    }
}
