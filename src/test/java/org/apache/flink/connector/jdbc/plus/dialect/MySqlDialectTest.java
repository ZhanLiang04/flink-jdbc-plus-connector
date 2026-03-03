package org.apache.flink.connector.jdbc.plus.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Pure unit tests for {@link MySqlDialect} SQL generation. No database connection required. */
class MySqlDialectTest {

    private MySqlDialect dialect;

    @BeforeEach
    void setUp() {
        dialect = MySqlDialect.INSTANCE;
    }

    // -------------------------------------------------------------------------
    // quoteIdentifier / quoteFullTableName
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("quoteIdentifier")
    class QuoteIdentifierTests {

        @Test
        void simpleColumn() {
            assertEquals("`id`", dialect.quoteIdentifier("id"));
        }

        @Test
        void columnWithBacktick() {
            assertEquals("`col``name`", dialect.quoteIdentifier("col`name"));
        }

        @Test
        void fullTableName() {
            assertEquals("`mydb`.`orders`", dialect.quoteFullTableName("mydb.orders"));
        }

        @Test
        void plainTableNameWithoutDatabase() {
            assertEquals("`orders`", dialect.quoteFullTableName("orders"));
        }
    }

    // -------------------------------------------------------------------------
    // buildMinMaxQuery
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("buildMinMaxQuery")
    class MinMaxQueryTests {

        @Test
        void standard() {
            String sql = dialect.buildMinMaxQuery("mydb.orders", "id");
            assertEquals("SELECT MIN(`id`), MAX(`id`) FROM `mydb`.`orders`", sql);
        }
    }

    // -------------------------------------------------------------------------
    // buildNextBoundaryQuery
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("buildNextBoundaryQuery")
    class NextBoundaryQueryTests {

        @Test
        void firstChunk_startIsNull() {
            String sql = dialect.buildNextBoundaryQuery("mydb.orders", "id", null, 8096);
            assertEquals("SELECT `id` FROM `mydb`.`orders` ORDER BY `id` LIMIT 8096, 1", sql);
        }

        @Test
        void middleChunk_startNotNull() {
            String sql = dialect.buildNextBoundaryQuery("mydb.orders", "id", 1000L, 8096);
            assertEquals(
                    "SELECT `id` FROM `mydb`.`orders` WHERE `id` > ? ORDER BY `id` LIMIT 8096, 1",
                    sql);
        }

        @Test
        void stringKeyChunk() {
            String sql = dialect.buildNextBoundaryQuery("mydb.customers", "name", "Alice", 500);
            assertEquals(
                    "SELECT `name` FROM `mydb`.`customers` WHERE `name` > ? ORDER BY `name` LIMIT 500, 1",
                    sql);
        }
    }

    // -------------------------------------------------------------------------
    // buildSplitScanQuery
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("buildSplitScanQuery")
    class SplitScanQueryTests {

        @Test
        void soleSplit_noStartNoEnd_noOffset() {
            JdbcSourceSplit split =
                    new JdbcSourceSplit("mydb.orders#0", "mydb.orders", "id", null, null);
            String sql = dialect.buildSplitScanQuery(split, "*", 1024);
            assertEquals("SELECT * FROM `mydb`.`orders` LIMIT 1024", sql);
        }

        @Test
        void firstChunk_endOnly() {
            JdbcSourceSplit split =
                    new JdbcSourceSplit("mydb.orders#0", "mydb.orders", "id", null, 1000L);
            String sql = dialect.buildSplitScanQuery(split, "*", 1024);
            assertEquals("SELECT * FROM `mydb`.`orders` WHERE `id` <= ? LIMIT 1024", sql);
        }

        @Test
        void lastChunk_startOnly() {
            JdbcSourceSplit split =
                    new JdbcSourceSplit("mydb.orders#9", "mydb.orders", "id", 9000L, null);
            String sql = dialect.buildSplitScanQuery(split, "*", 1024);
            assertEquals("SELECT * FROM `mydb`.`orders` WHERE `id` > ? LIMIT 1024", sql);
        }

        @Test
        void middleChunk_bothBounds() {
            JdbcSourceSplit split =
                    new JdbcSourceSplit("mydb.orders#1", "mydb.orders", "id", 1000L, 2000L);
            String sql = dialect.buildSplitScanQuery(split, "*", 1024);
            assertEquals(
                    "SELECT * FROM `mydb`.`orders` WHERE `id` > ? AND `id` <= ? LIMIT 1024", sql);
        }

        @Test
        void withOffset_resumeAfterFailover() {
            JdbcSourceSplit split =
                    new JdbcSourceSplit("mydb.orders#1", "mydb.orders", "id", 1000L, 2000L, 512L);
            String sql = dialect.buildSplitScanQuery(split, "*", 1024);
            assertEquals(
                    "SELECT * FROM `mydb`.`orders` WHERE `id` > ? AND `id` <= ? LIMIT 1024 OFFSET 512",
                    sql);
        }

        @Test
        void noFetchSize_withOffset() {
            JdbcSourceSplit split =
                    new JdbcSourceSplit("mydb.orders#1", "mydb.orders", "id", 1000L, 2000L, 256L);
            String sql = dialect.buildSplitScanQuery(split, "*", 0);
            assertTrue(
                    sql.contains("OFFSET 256"),
                    "Should contain OFFSET when fetchSize=0 but offset>0");
        }

        @Test
        void customColumnList() {
            JdbcSourceSplit split =
                    new JdbcSourceSplit("mydb.orders#0", "mydb.orders", "id", null, 500L);
            String sql = dialect.buildSplitScanQuery(split, "`id`, `amount`, `status`", 1024);
            assertTrue(
                    sql.startsWith("SELECT `id`, `amount`, `status` FROM"),
                    "Should use custom column list");
        }
    }

    // -------------------------------------------------------------------------
    // buildTableListQuery / buildPrimaryKeyQuery
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("schema discovery queries")
    class SchemaDiscoveryTests {

        @Test
        void tableListQuery() {
            String sql = dialect.buildTableListQuery();
            assertTrue(
                    sql.contains("information_schema.TABLES"), "Should query information_schema");
            assertTrue(sql.contains("TABLE_SCHEMA = ?"), "Should filter by schema");
            assertTrue(sql.contains("BASE TABLE"), "Should filter for base tables only");
        }

        @Test
        void primaryKeyQuery() {
            String sql = dialect.buildPrimaryKeyQuery("mydb", "orders");
            assertTrue(sql.contains("KEY_COLUMN_USAGE"), "Should query KEY_COLUMN_USAGE");
            assertTrue(sql.contains("mydb"), "Should filter by database");
            assertTrue(sql.contains("orders"), "Should filter by table");
            assertTrue(sql.contains("PRIMARY"), "Should filter for PRIMARY KEY");
            assertTrue(sql.contains("LIMIT 1"), "Should return only the first PK column");
        }
    }
}
