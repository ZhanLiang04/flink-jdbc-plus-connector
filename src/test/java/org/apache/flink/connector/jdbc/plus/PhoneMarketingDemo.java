package org.apache.flink.connector.jdbc.plus;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.LocalDateTime;

/**
 * Demo：通过 flink-jdbc-plus 读取 StarRocks phone_marketing_manage 表，结果用 print 输出。
 *
 * <p>StarRocks 兼容 MySQL 协议，默认查询端口 9030，直接使用 MySQL JDBC 驱动即可。
 *
 * <p>表字段（42 列，与 {@link StarRocksPhoneDataGenDemo} 写入时的结构一致）：
 *
 * <pre>
 *  #   字段名                      StarRocks 类型       Flink 映射类型
 *  01  phone_md5                   VARCHAR(50)          STRING       ← AGGREGATE KEY（VARCHAR 主键）
 *  02  phone                       VARCHAR(32)          STRING
 *  03  province                    VARCHAR(32)          STRING
 *  04  city                        VARCHAR(64)          STRING
 *  05  company                     VARCHAR(32)          STRING
 *  06  province_code               VARCHAR(16)          STRING
 *  07  city_code                   VARCHAR(16)          STRING
 *  08  limit_expired_time          DATETIME             TIMESTAMP(0)  可为 NULL
 *  09  number_not_exist            TINYINT              TINYINT
 *  10  number_segment_not_opened   TINYINT              TINYINT
 *  11  number_segment_generation   VARCHAR(10)          STRING
 *  12  number_segment_type         TINYINT              TINYINT
 *  13  identify_number             VARCHAR(32)          STRING
 *  14  birthday                    VARCHAR(32)          STRING
 *  15  gender                      TINYINT              TINYINT
 *  16  name                        VARCHAR(64)          STRING
 *  17  belonging                   VARCHAR(32)          STRING
 *  18  batch_no                    VARCHAR(32)          STRING
 *  19  score                       VARCHAR(11)          STRING
 *  20  score_time                  DATETIME             TIMESTAMP(0)  可为 NULL
 *  21  score_channel               VARCHAR(32)          STRING
 *  22  score_model                 VARCHAR(32)          STRING
 *  23  is_black                    TINYINT              TINYINT
 *  24  is_insured                  TINYINT              TINYINT
 *  25  last_used_time              DATETIME             TIMESTAMP(0)
 *  26  last_call_time              DATETIME             TIMESTAMP(0)
 *  27  call_count                  INT                  INT
 *  28  call_success_count          INT                  INT
 *  29  call_failed_count           INT                  INT
 *  30  last_message_status         TINYINT              TINYINT       可为 NULL
 *  31  message_count               INT                  INT
 *  32  message_success_count       INT                  INT
 *  33  last_message_time           DATETIME             TIMESTAMP(0)  可为 NULL
 *  34  saf_score                   VARCHAR(11)          STRING
 *  35  saf_score_time              DATETIME             TIMESTAMP(0)
 *  36  partition_score             VARCHAR(10)          STRING
 *  37  partition_score1            VARCHAR(10)          STRING
 *  38  partition_score2            VARCHAR(10)          STRING
 *  39  partition_score3            VARCHAR(10)          STRING
 *  40  partition_score4            VARCHAR(10)          STRING        可为 NULL
 *  41  traffic_partition_time      VARCHAR(20)          STRING
 *  42  partition_estimate_birthday VARCHAR(10)          STRING
 * </pre>
 *
 * <p>注意：phone_marketing_manage 的聚合键 phone_md5 为 VARCHAR 类型，无法作为数值切分键。 本 Demo 将 chunk-size
 * 设置为远大于实际行数的值（100000），使连接器以单分片读取全表， 从而绕过数值分片的限制。如需多分片并行，可在表中增加一列数值类型的自增或哈希辅助键。
 *
 * <p>运行方式（先确保已执行 {@link StarRocksPhoneDataGenDemo} 写入数据）：
 *
 * <pre>
 *   mvn test-compile
 *   mvn exec:java \
 *     -Dexec.mainClass="org.apache.flink.connector.jdbc.plus.PhoneMarketingDemo" \
 *     -Dexec.classpathScope="test"
 * </pre>
 */
public class PhoneMarketingDemo {

    // ── StarRocks 连接信息（按实际环境修改）────────────────────────────────────
    static final String JDBC_URL =
            "jdbc:mysql://11.183.212.126:9030/test"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
                    + "&characterEncoding=UTF-8";
    static final String USERNAME = "root";
    static final String PASSWORD = "123456";
    static final String DATABASE = "test"; // ← 与 JDBC_URL 中的库名保持一致
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

        // ── 源表 DDL（42 列，与 StarRocks 表结构严格对应）─────────────────────
        tenv.executeSql(
                "CREATE TABLE phone_marketing_src ("
                        + "  phone_md5                   STRING,"
                        + "  phone                       STRING,"
                        + "  province                    STRING,"
                        + "  city                        STRING,"
                        + "  company                     STRING,"
                        + "  province_code               STRING,"
                        + "  city_code                   STRING,"
                        + "  limit_expired_time          TIMESTAMP(0),"
                        + "  number_not_exist            TINYINT,"
                        + "  number_segment_not_opened   TINYINT,"
                        + "  number_segment_generation   STRING,"
                        + "  number_segment_type         TINYINT,"
                        + "  identify_number             STRING,"
                        + "  birthday                    STRING,"
                        + "  gender                      TINYINT,"
                        + "  name                        STRING,"
                        + "  belonging                   STRING,"
                        + "  batch_no                    STRING,"
                        + "  score                       STRING,"
                        + "  score_time                  TIMESTAMP(0),"
                        + "  score_channel               STRING,"
                        + "  score_model                 STRING,"
                        + "  is_black                    TINYINT,"
                        + "  is_insured                  TINYINT,"
                        + "  last_used_time              TIMESTAMP(0),"
                        + "  last_call_time              TIMESTAMP(0),"
                        + "  call_count                  INT,"
                        + "  call_success_count          INT,"
                        + "  call_failed_count           INT,"
                        + "  last_message_status         TINYINT,"
                        + "  message_count               INT,"
                        + "  message_success_count       INT,"
                        + "  last_message_time           TIMESTAMP(0),"
                        + "  saf_score                   STRING,"
                        + "  saf_score_time              TIMESTAMP(0),"
                        + "  partition_score             STRING,"
                        + "  partition_score1            STRING,"
                        + "  partition_score2            STRING,"
                        + "  partition_score3            STRING,"
                        + "  partition_score4            STRING,"
                        + "  traffic_partition_time      STRING,"
                        + "  partition_estimate_birthday STRING"
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
                        + "  'table-list' = 'phone_marketing_manage',"
                        // phone_md5 为 VARCHAR，无法数值切分；chunk-size 设大于总行数，全表单分片读取
                        + "  'chunk-size' = '500',"
                        + "  'split-key-column' = 'phone_md5'"
                        + ")");

        // ── Print Sink（结构与源表完全一致）───────────────────────────────────
        tenv.executeSql(
                "CREATE TABLE sink_print ("
                        + "  phone_md5                   STRING,"
                        + "  phone                       STRING,"
                        + "  province                    STRING,"
                        + "  city                        STRING,"
                        + "  company                     STRING,"
                        + "  province_code               STRING,"
                        + "  city_code                   STRING,"
                        + "  limit_expired_time          TIMESTAMP(0),"
                        + "  number_not_exist            TINYINT,"
                        + "  number_segment_not_opened   TINYINT,"
                        + "  number_segment_generation   STRING,"
                        + "  number_segment_type         TINYINT,"
                        + "  identify_number             STRING,"
                        + "  birthday                    STRING,"
                        + "  gender                      TINYINT,"
                        + "  name                        STRING,"
                        + "  belonging                   STRING,"
                        + "  batch_no                    STRING,"
                        + "  score                       STRING,"
                        + "  score_time                  TIMESTAMP(0),"
                        + "  score_channel               STRING,"
                        + "  score_model                 STRING,"
                        + "  is_black                    TINYINT,"
                        + "  is_insured                  TINYINT,"
                        + "  last_used_time              TIMESTAMP(0),"
                        + "  last_call_time              TIMESTAMP(0),"
                        + "  call_count                  INT,"
                        + "  call_success_count          INT,"
                        + "  call_failed_count           INT,"
                        + "  last_message_status         TINYINT,"
                        + "  message_count               INT,"
                        + "  message_success_count       INT,"
                        + "  last_message_time           TIMESTAMP(0),"
                        + "  saf_score                   STRING,"
                        + "  saf_score_time              TIMESTAMP(0),"
                        + "  partition_score             STRING,"
                        + "  partition_score1            STRING,"
                        + "  partition_score2            STRING,"
                        + "  partition_score3            STRING,"
                        + "  partition_score4            STRING,"
                        + "  traffic_partition_time      STRING,"
                        + "  partition_estimate_birthday STRING"
                        + ") WITH ("
                        + "  'connector' = 'print'"
                        + ")");

        // 触发 Flink Job 执行
        tenv.executeSql("select count(*) from phone_marketing_src").print();
        //        tenv.executeSql("INSERT INTO sink_print SELECT * FROM
        // phone_marketing_src").await();
    }

    // =========================================================================
    // 用法二：DataStream API
    // =========================================================================

    static void runDataStreamDemo() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(/*parallelism=*/ 1);

        // RowType 字段顺序 / 类型必须与 StarRocks 列顺序完全一致（共 42 列）
        RowType rowType =
                RowType.of(
                        new VarCharType(50), //  01 phone_md5
                        new VarCharType(32), //  02 phone
                        new VarCharType(32), //  03 province
                        new VarCharType(64), //  04 city
                        new VarCharType(32), //  05 company
                        new VarCharType(16), //  06 province_code
                        new VarCharType(16), //  07 city_code
                        new TimestampType(0), //  08 limit_expired_time
                        new TinyIntType(), //  09 number_not_exist
                        new TinyIntType(), //  10 number_segment_not_opened
                        new VarCharType(10), //  11 number_segment_generation
                        new TinyIntType(), //  12 number_segment_type
                        new VarCharType(32), //  13 identify_number
                        new VarCharType(32), //  14 birthday
                        new TinyIntType(), //  15 gender
                        new VarCharType(64), //  16 name
                        new VarCharType(32), //  17 belonging
                        new VarCharType(32), //  18 batch_no
                        new VarCharType(11), //  19 score
                        new TimestampType(0), //  20 score_time
                        new VarCharType(32), //  21 score_channel
                        new VarCharType(32), //  22 score_model
                        new TinyIntType(), //  23 is_black
                        new TinyIntType(), //  24 is_insured
                        new TimestampType(0), //  25 last_used_time
                        new TimestampType(0), //  26 last_call_time
                        new IntType(), //  27 call_count
                        new IntType(), //  28 call_success_count
                        new IntType(), //  29 call_failed_count
                        new TinyIntType(), //  30 last_message_status
                        new IntType(), //  31 message_count
                        new IntType(), //  32 message_success_count
                        new TimestampType(0), //  33 last_message_time
                        new VarCharType(11), //  34 saf_score
                        new TimestampType(0), //  35 saf_score_time
                        new VarCharType(10), //  36 partition_score
                        new VarCharType(10), //  37 partition_score1
                        new VarCharType(10), //  38 partition_score2
                        new VarCharType(10), //  39 partition_score3
                        new VarCharType(10), //  40 partition_score4
                        new VarCharType(20), //  41 traffic_partition_time
                        new VarCharType(10) //  42 partition_estimate_birthday
                        );

        JdbcPlusSource source =
                JdbcPlusSource.builder()
                        .url(JDBC_URL)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .database(DATABASE)
                        .tableList("phone_marketing_manage")
                        // phone_md5 为 VARCHAR，设大 chunk-size 令全表走单分片
                        .chunkSize(100_000)
                        .fetchSize(1000)
                        .rowType(rowType)
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "phone-marketing-source")
                .map(row -> formatRow(row, rowType))
                .print();

        env.execute("PhoneMarketingDemo - DataStream");
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
                case INTEGER:
                    sb.append(row.getInt(i));
                    break;
                case TINYINT:
                    sb.append(row.getByte(i));
                    break;
                case CHAR:
                case VARCHAR:
                    sb.append(row.getString(i));
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    {
                        int precision = ((TimestampType) rowType.getTypeAt(i)).getPrecision();
                        TimestampData ts = row.getTimestamp(i, precision);
                        LocalDateTime ldt = ts.toLocalDateTime();
                        sb.append(ldt);
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
