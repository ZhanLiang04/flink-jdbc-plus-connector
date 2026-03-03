package org.apache.flink.connector.jdbc.plus;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Demo：通过 flink-jdbc-plus 读取 MySQL 多张表（t_user05 / t_user08），结果用 print 输出。
 *
 * <p>表结构（两张表结构相同）：
 *
 * <pre>
 *   id    BIGINT        -- 主键，非均匀切分 split key
 *   name  VARCHAR(...)  -- 姓名
 *   birth DATETIME      -- 生日
 * </pre>
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn test-compile
 *   mvn exec:java -Dexec.mainClass="org.apache.flink.connector.jdbc.plus.JdbcPlusDemo" \
 *                 -Dexec.classpathScope="test"
 * </pre>
 */
public class JdbcPlusDemo {

    // ── 连接信息 ───────────────────────────────────────────────────────────────
    static final String JDBC_URL =
            "jdbc:mysql://localhost:13306/db_sync"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    static final String USERNAME = "root";
    static final String PASSWORD = "123456"; // ← 按实际修改
    static final String DATABASE = "db_sync";
    // ──────────────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        runTableSqlDemo();
        // runDataStreamDemo();
    }

    // =========================================================================
    // 用法一：Table / SQL API
    // =========================================================================

    static void runTableSqlDemo() throws Exception {
        TableEnvironment tenv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        // DDL 字段顺序 / 类型必须与 SELECT * 的实际列顺序一致
        tenv.executeSql(
                "CREATE TABLE t_user ("
                        + "  id    BIGINT,"
                        + "  name  STRING,"
                        + "  birth TIMESTAMP(3)"
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
                        + "  'table-list' = 't_user05,t_user08',"
                        + "  'chunk-size' = '1000'"
                        + ")");
        // 注意：字段类型和名称必须与源表 t_user 保持一致，或者在 INSERT 时进行转换
        tenv.executeSql(
                "CREATE TABLE sink_print ("
                        + "  id    BIGINT,"
                        + "  name  STRING,"
                        + "  birth TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'print'"
                        + ")");

        // 2. 编写插入语句将数据从源表导入 Sink
        // 这会触发 Flink Job 的执行
        tenv.executeSql("INSERT INTO sink_print SELECT * FROM t_user");

        /*
                System.out.println("=== SELECT * FROM t_user05 + t_user08 ===");
                tenv.executeSql("SELECT * FROM t_user").print();

                System.out.println("=== 每张表的行数统计 ===");
                tenv.executeSql("SELECT COUNT(*) AS total_rows FROM t_user").print();
        */
    }

    // =========================================================================
    // 用法二：DataStream API（字段类型与表结构对齐）
    // =========================================================================

    static void runDataStreamDemo() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(/*parallelism=*/ 2);

        // RowType 字段顺序和类型必须与 SELECT * 返回列一致：
        //   id BIGINT | name VARCHAR | birth TIMESTAMP(3)
        RowType rowType =
                RowType.of(
                        new BigIntType(), // id
                        new VarCharType(VarCharType.MAX_LENGTH), // name
                        new TimestampType(3)); // birth

        JdbcPlusSource source =
                JdbcPlusSource.builder()
                        .url(JDBC_URL)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .database(DATABASE)
                        .tableList("t_user05", "t_user08")
                        .chunkSize(1000)
                        .fetchSize(256)
                        .rowType(rowType)
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "jdbc-plus-source")
                .map(row -> formatRow(row, rowType))
                .print();

        env.execute("JdbcPlus Demo - DataStream");
    }

    // =========================================================================
    // 辅助：RowData → 可读字符串（用于 DataStream 模式的 print）
    // =========================================================================

    private static String formatRow(RowData row, RowType rowType) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) sb.append(", ");
            if (row.isNullAt(i)) {
                sb.append("null");
                continue;
            }
            switch (rowType.getTypeAt(i).getTypeRoot()) {
                case BIGINT:
                    sb.append(row.getLong(i));
                    break;
                case VARCHAR:
                case CHAR:
                    sb.append(row.getString(i));
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    {
                        int precision = ((TimestampType) rowType.getTypeAt(i)).getPrecision();
                        TimestampData ts = row.getTimestamp(i, precision);
                        sb.append(ts.toLocalDateTime());
                        break;
                    }
                default:
                    sb.append(row.getString(i));
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
