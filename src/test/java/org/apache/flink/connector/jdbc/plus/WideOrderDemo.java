package org.apache.flink.connector.jdbc.plus;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.LocalDate;

/**
 * Demo：通过 flink-jdbc-plus 读取 t_wide_order 宽表，结果用 print 输出。
 *
 * <p>对应 {@link DataGenDemo#DDL} 建表后的 20 列表结构：
 *
 * <pre>
 *   id           BIGINT         -- PK，非均匀大跨度随机值
 *   order_no     VARCHAR(32)
 *   user_id      BIGINT
 *   status       TINYINT        -- 0~4
 *   amount       DECIMAL(12,2)
 *   discount     FLOAT
 *   score        DOUBLE
 *   is_deleted   BIT(1)         -- Flink 中映射为 BOOLEAN
 *   remark       TEXT           -- Flink 中映射为 STRING（可为 NULL）
 *   category     CHAR(4)
 *   tags         VARCHAR(200)   -- 可为 NULL
 *   quantity     INT
 *   weight       DECIMAL(8,3)
 *   product_id   BIGINT
 *   region_code  SMALLINT
 *   priority     TINYINT UNSIGNED -- Flink 中映射为 SMALLINT（无符号 0~255）
 *   extra_json   JSON           -- Flink 中映射为 STRING（可为 NULL）
 *   order_date   DATE
 *   pay_time     DATETIME(3)    -- 可为 NULL
 *   updated_at   TIMESTAMP(3)
 * </pre>
 *
 * <p>运行方式（先确保已执行 {@link DataGenDemo} 写入数据）：
 *
 * <pre>
 *   mvn test-compile
 *   mvn exec:java -Dexec.mainClass="org.apache.flink.connector.jdbc.plus.WideOrderDemo" \
 *                 -Dexec.classpathScope="test"
 * </pre>
 */
public class WideOrderDemo {

    // ── 连接信息 ───────────────────────────────────────────────────────────────
    static final String JDBC_URL =
            "jdbc:mysql://localhost:13306/db_sync"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    static final String USERNAME = "root";
    static final String PASSWORD = "123456";
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

        // 源表 DDL —— 字段顺序 / 类型与 t_wide_order 保持一致
        tenv.executeSql(
                "CREATE TABLE wide_order_src ("
                        + "  id          BIGINT,"
                        + "  order_no    STRING,"
                        + "  user_id     BIGINT,"
                        + "  status      TINYINT,"
                        + "  amount      DECIMAL(12, 2),"
                        + "  discount    FLOAT,"
                        + "  score       DOUBLE,"
                        + "  is_deleted  BOOLEAN,"
                        + "  remark      STRING,"
                        + "  category    CHAR(4),"
                        + "  tags        STRING,"
                        + "  quantity    INT,"
                        + "  weight      DECIMAL(8, 3),"
                        + "  product_id  BIGINT,"
                        + "  region_code SMALLINT,"
                        + "  priority    SMALLINT," // TINYINT UNSIGNED → SMALLINT
                        + "  extra_json  STRING," // JSON → STRING
                        + "  order_date  DATE,"
                        + "  pay_time    TIMESTAMP(3),"
                        + "  updated_at  TIMESTAMP(3)"
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
                        + "  'table-list' = 't_wide_order',"
                        + "  'chunk-size' = '50'"
                        + ")");

        // Print Sink
        tenv.executeSql(
                "CREATE TABLE sink_print ("
                        + "  id          BIGINT,"
                        + "  order_no    STRING,"
                        + "  user_id     BIGINT,"
                        + "  status      TINYINT,"
                        + "  amount      DECIMAL(12, 2),"
                        + "  discount    FLOAT,"
                        + "  score       DOUBLE,"
                        + "  is_deleted  BOOLEAN,"
                        + "  remark      STRING,"
                        + "  category    CHAR(4),"
                        + "  tags        STRING,"
                        + "  quantity    INT,"
                        + "  weight      DECIMAL(8, 3),"
                        + "  product_id  BIGINT,"
                        + "  region_code SMALLINT,"
                        + "  priority    SMALLINT,"
                        + "  extra_json  STRING,"
                        + "  order_date  DATE,"
                        + "  pay_time    TIMESTAMP(3),"
                        + "  updated_at  TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'print'"
                        + ")");

        // 触发 Flink Job 执行
        tenv.executeSql("INSERT INTO sink_print SELECT * FROM wide_order_src").await();
    }

    // =========================================================================
    // 用法二：DataStream API
    // =========================================================================

    static void runDataStreamDemo() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(/*parallelism=*/ 1);

        // RowType 字段顺序 / 类型必须与 MySQL 列顺序完全一致
        RowType rowType =
                RowType.of(
                        new BigIntType(), // id
                        new VarCharType(32), // order_no
                        new BigIntType(), // user_id
                        new TinyIntType(), // status
                        new DecimalType(12, 2), // amount
                        new FloatType(), // discount
                        new DoubleType(), // score
                        new BooleanType(), // is_deleted (BIT(1))
                        new VarCharType(VarCharType.MAX_LENGTH), // remark (TEXT)
                        new CharType(4), // category
                        new VarCharType(200), // tags
                        new IntType(), // quantity
                        new DecimalType(8, 3), // weight
                        new BigIntType(), // product_id
                        new SmallIntType(), // region_code
                        new SmallIntType(), // priority (TINYINT UNSIGNED)
                        new VarCharType(VarCharType.MAX_LENGTH), // extra_json (JSON)
                        new DateType(), // order_date
                        new TimestampType(3), // pay_time
                        new TimestampType(3) // updated_at
                        );

        JdbcPlusSource source =
                JdbcPlusSource.builder()
                        .url(JDBC_URL)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .database(DATABASE)
                        .tableList("t_wide_order")
                        .chunkSize(50)
                        .fetchSize(256)
                        .rowType(rowType)
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "wide-order-source")
                .map(row -> formatRow(row, rowType))
                .print();

        env.execute("WideOrderDemo - DataStream");
    }

    // =========================================================================
    // 辅助：RowData → 可读字符串
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
                case INTEGER:
                    sb.append(row.getInt(i));
                    break;
                case SMALLINT:
                    sb.append(row.getShort(i));
                    break;
                case TINYINT:
                    sb.append(row.getByte(i));
                    break;
                case FLOAT:
                    sb.append(row.getFloat(i));
                    break;
                case DOUBLE:
                    sb.append(row.getDouble(i));
                    break;
                case BOOLEAN:
                    sb.append(row.getBoolean(i));
                    break;
                case DECIMAL:
                    {
                        DecimalType dt = (DecimalType) rowType.getTypeAt(i);
                        DecimalData d = row.getDecimal(i, dt.getPrecision(), dt.getScale());
                        sb.append(d.toBigDecimal().toPlainString());
                        break;
                    }
                case CHAR:
                case VARCHAR:
                    sb.append(row.getString(i));
                    break;
                case DATE:
                    // Flink DATE 以 epoch day（int）存储
                    sb.append(LocalDate.ofEpochDay(row.getInt(i)));
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
