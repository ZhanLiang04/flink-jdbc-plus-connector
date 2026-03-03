package org.apache.flink.connector.jdbc.plus;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

/**
 * StarRocks 数据生成 Demo：向 phone_marketing_manage 表批量插入 10,000 条随机营销数据。
 *
 * <p>StarRocks 兼容 MySQL 协议，直接使用 MySQL JDBC 驱动即可连接。
 *
 * <p>表为 AGGREGATE KEY 聚合表，以 phone_md5 为聚合键；本 Demo 每个手机号生成一条记录， 不会触发聚合合并，便于观察数据写入效果。
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn test-compile
 *   mvn exec:java \
 *     -Dexec.mainClass="org.apache.flink.connector.jdbc.plus.StarRocksPhoneDataGenDemo" \
 *     -Dexec.classpathScope="test"
 * </pre>
 */
public class StarRocksPhoneDataGenDemo {

    // ── StarRocks 连接信息（按实际环境修改）────────────────────────────────────
    // StarRocks 默认 MySQL 兼容端口为 9030
    static final String JDBC_URL =
            "jdbc:mysql://11.183.212.126:9030/test"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
                    + "&characterEncoding=UTF-8";
    static final String USERNAME = "root";
    static final String PASSWORD = "123456"; // StarRocks 默认 root 无密码
    // ──────────────────────────────────────────────────────────────────────────

    /** 写入总行数 */
    static final int ROW_COUNT = 10_000;

    /** 每批提交行数（StarRocks 推荐较大 batch，减少事务开销） */
    static final int BATCH_SIZE = 500;

    // ── INSERT SQL（42 列，与建表语句完全对应）──────────────────────────────────
    private static final String INSERT_SQL =
            "INSERT INTO phone_marketing_manage ("
                    + "phone_md5, phone, province, city, company, province_code, city_code,"
                    + "limit_expired_time, number_not_exist, number_segment_not_opened,"
                    + "number_segment_generation, number_segment_type,"
                    + "identify_number, birthday, gender, name,"
                    + "belonging, batch_no, score, score_time, score_channel, score_model,"
                    + "is_black, is_insured,"
                    + "last_used_time, last_call_time,"
                    + "call_count, call_success_count, call_failed_count,"
                    + "last_message_status, message_count, message_success_count, last_message_time,"
                    + "saf_score, saf_score_time,"
                    + "partition_score, partition_score1, partition_score2,"
                    + "partition_score3, partition_score4,"
                    + "traffic_partition_time, partition_estimate_birthday"
                    + ") VALUES ("
                    + "?,?,?,?,?,?,?,"
                    + "?,?,?,"
                    + "?,?,"
                    + "?,?,?,?,"
                    + "?,?,?,?,?,?,"
                    + "?,?,"
                    + "?,?,"
                    + "?,?,?,"
                    + "?,?,?,?,"
                    + "?,?,"
                    + "?,?,?,"
                    + "?,?,"
                    + "?,?"
                    + ")";

    // ── 静态数据池 ─────────────────────────────────────────────────────────────

    private static final String[][] PROVINCE_CITY = {
        {"北京", "北京", "110000", "110100"},
        {"上海", "上海", "310000", "310100"},
        {"广东", "广州", "440000", "440100"},
        {"广东", "深圳", "440000", "440300"},
        {"广东", "佛山", "440000", "440600"},
        {"浙江", "杭州", "330000", "330100"},
        {"浙江", "宁波", "330000", "330200"},
        {"江苏", "南京", "320000", "320100"},
        {"江苏", "苏州", "320000", "320500"},
        {"四川", "成都", "510000", "510100"},
        {"湖北", "武汉", "420000", "420100"},
        {"陕西", "西安", "610000", "610100"},
        {"河南", "郑州", "410000", "410100"},
        {"湖南", "长沙", "430000", "430100"},
        {"福建", "福州", "350000", "350100"},
        {"福建", "厦门", "350000", "350200"},
        {"山东", "济南", "370000", "370100"},
        {"山东", "青岛", "370000", "370200"},
        {"辽宁", "沈阳", "210000", "210100"},
        {"重庆", "重庆", "500000", "500100"},
    };

    private static final String[] COMPANIES = {"中国移动", "中国联通", "中国电信", "中国广电"};

    // 手机号段前缀（按运营商分组）
    private static final String[][] PREFIXES = {
        // 移动
        {
            "134", "135", "136", "137", "138", "139", "147", "150", "151", "152", "157", "158",
            "159", "172", "178", "182", "183", "184", "187", "188", "195", "197", "198"
        },
        // 联通
        {"130", "131", "132", "145", "155", "156", "166", "175", "176", "185", "186", "196"},
        // 电信
        {"133", "149", "153", "173", "174", "177", "180", "181", "189", "191", "193", "199"},
        // 广电
        {"192"},
    };

    private static final String[] GENERATIONS = {"2G", "3G", "4G", "5G"};

    private static final String[] BELONGINGS = {
        "A(2Y)", "B(10Y)", "C(自营)", "D(合作)", "E(采购)", "F(5Y)"
    };

    private static final String[] LAST_NAMES = {
        "王", "李", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴",
        "徐", "孙", "胡", "朱", "高", "林", "何", "郭", "马", "罗"
    };

    private static final String[] FIRST_NAMES = {
        "伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "洋",
        "艳", "勇", "军", "杰", "娟", "涛", "明", "超", "秀兰", "霞",
        "平", "刚", "桂英", "华", "玲", "文", "彬", "辉", "建国", "阳"
    };

    private static final DateTimeFormatter DT_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ── main ──────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        System.out.printf("开始向 phone_marketing_manage 写入 %,d 条数据...%n", ROW_COUNT);
        long t0 = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                Random rnd = new Random();
                for (int i = 0; i < ROW_COUNT; i++) {
                    fillRow(ps, rnd);
                    ps.addBatch();

                    if ((i + 1) % BATCH_SIZE == 0 || i == ROW_COUNT - 1) {
                        ps.executeBatch();
                        conn.commit();
                        System.out.printf(
                                "  已提交 %,d / %,d 行%n", Math.min(i + 1, ROW_COUNT), ROW_COUNT);
                    }
                }
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }

        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("✓ 写入完成，共 %,d 行，耗时 %.1f 秒%n", ROW_COUNT, elapsed / 1000.0);
    }

    // ── 行数据填充 ─────────────────────────────────────────────────────────────

    private static void fillRow(PreparedStatement ps, Random rnd) throws SQLException {
        // -- 手机号 & MD5 --
        int companyIdx = rnd.nextInt(COMPANIES.length);
        String[] prefixPool = PREFIXES[companyIdx];
        String prefix = prefixPool[rnd.nextInt(prefixPool.length)];
        String phone = prefix + String.format("%08d", rnd.nextInt(100_000_000));
        String phoneMd5 = md5(phone);

        // -- 地区 --
        String[] pc = PROVINCE_CITY[rnd.nextInt(PROVINCE_CITY.length)];
        String province = pc[0];
        String city = pc[1];
        String provinceCode = pc[2];
        String cityCode = pc[3];

        // -- 时间基准：近 3 年内随机 --
        LocalDateTime baseTime =
                LocalDateTime.now().minusDays(rnd.nextInt(1095)).minusHours(rnd.nextInt(24));

        // -- 身份证 & 生日 & 性别 --
        int birthYear = 1960 + rnd.nextInt(45); // 1960~2004
        int birthMonth = rnd.nextInt(12) + 1;
        int birthDay = rnd.nextInt(28) + 1;
        String birthday = String.format("%04d-%02d-%02d", birthYear, birthMonth, birthDay);
        int gender = rnd.nextInt(2) + 1; // 1男 2女
        String idSeq = String.format("%03d", rnd.nextInt(999) + 1);
        // 末位用 X 或数字模拟（非真实校验）
        String idCheck = rnd.nextInt(10) == 0 ? "X" : String.valueOf(rnd.nextInt(10));
        String idNumber =
                provinceCode.substring(0, 6)
                        + String.format("%04d%02d%02d", birthYear, birthMonth, birthDay)
                        + idSeq
                        + idCheck;

        // -- 姓名 --
        String name =
                LAST_NAMES[rnd.nextInt(LAST_NAMES.length)]
                        + FIRST_NAMES[rnd.nextInt(FIRST_NAMES.length)];

        // -- 评分类信息 --
        String score = String.format("%.4f", 0.3 + rnd.nextDouble() * 0.6); // 0.3~0.9
        String safScore = String.format("%.4f", 0.2 + rnd.nextDouble() * 0.7);
        LocalDateTime scoreTime = baseTime.minusDays(rnd.nextInt(30));

        // -- 分层分数 --
        String partScore = String.format("%.1f", rnd.nextDouble());
        String partScore1 = partitionScore1(rnd);
        String partScore2 = String.format("%.3f", rnd.nextDouble());
        String partScore3 = String.format("%.3f", rnd.nextDouble());
        String partScore4 = rnd.nextBoolean() ? String.format("%d", 45 + rnd.nextInt(21)) : null;

        // -- 呼叫 / 短信统计 --
        int callCount = rnd.nextInt(50);
        int callSuccess = rnd.nextInt(callCount + 1);
        int callFailed = callCount - callSuccess;
        int msgCount = rnd.nextInt(20);
        int msgSuccess = rnd.nextInt(msgCount + 1);
        LocalDateTime lastCallTime = baseTime.minusHours(rnd.nextInt(720));
        LocalDateTime lastUsedTime =
                lastCallTime.plusHours(rnd.nextInt(24)).isAfter(LocalDateTime.now())
                        ? lastCallTime
                        : lastCallTime.plusHours(rnd.nextInt(24));
        LocalDateTime lastMsgTime = msgSuccess > 0 ? baseTime.minusHours(rnd.nextInt(360)) : null;

        // -- 批次 & 归属 --
        String belonging = BELONGINGS[rnd.nextInt(BELONGINGS.length)];
        String batchNo = "BATCH-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        // -- 限制到期时间（部分有限制） --
        LocalDateTime limitExpired =
                rnd.nextInt(5) == 0 ? LocalDateTime.now().plusDays(rnd.nextInt(180)) : null;

        // -- 年龄分层日期 --
        String trafficPartitionTime = LocalDate.now().minusDays(rnd.nextInt(365)).toString();

        // -- 估算生日（分层反推，精确到月）--
        String estimateBirthday = String.format("%04d-%02d", birthYear, birthMonth);

        // ── 按 INSERT SQL 中的列顺序逐一 set ──────────────────────────────────
        int col = 1;
        ps.setString(col++, phoneMd5);
        ps.setString(col++, phone);
        ps.setString(col++, province);
        ps.setString(col++, city);
        ps.setString(col++, COMPANIES[companyIdx]);
        ps.setString(col++, provinceCode);
        ps.setString(col++, cityCode);
        setDateTimeOrNull(ps, col++, limitExpired);
        ps.setByte(col++, (byte) (rnd.nextInt(20) == 0 ? 1 : 0)); // number_not_exist
        ps.setByte(col++, (byte) (rnd.nextInt(30) == 0 ? 1 : 0)); // segment_not_opened
        ps.setString(col++, GENERATIONS[rnd.nextInt(GENERATIONS.length)]);
        ps.setByte(col++, (byte) (rnd.nextInt(4) + 1)); // segment_type 1~4
        ps.setString(col++, idNumber);
        ps.setString(col++, birthday);
        ps.setByte(col++, (byte) gender);
        ps.setString(col++, name);
        ps.setString(col++, belonging);
        ps.setString(col++, batchNo);
        ps.setString(col++, score);
        ps.setString(col++, scoreTime.format(DT_FMT));
        ps.setString(col++, "SAF");
        ps.setString(col++, "MOFANG");
        ps.setByte(col++, (byte) (rnd.nextInt(100) < 5 ? 1 : 0)); // is_black 5%
        ps.setByte(col++, (byte) (rnd.nextInt(10) < 3 ? 1 : 0)); // is_insured 30%
        ps.setString(col++, lastUsedTime.format(DT_FMT));
        ps.setString(col++, lastCallTime.format(DT_FMT));
        ps.setInt(col++, callCount);
        ps.setInt(col++, callSuccess);
        ps.setInt(col++, callFailed);
        ps.setByte(col++, (byte) (msgSuccess > 0 ? 1 : 0)); // last_message_status
        ps.setInt(col++, msgCount);
        ps.setInt(col++, msgSuccess);
        setDateTimeOrNull(ps, col++, lastMsgTime);
        ps.setString(col++, safScore);
        ps.setString(col++, scoreTime.minusDays(1).format(DT_FMT));
        ps.setString(col++, partScore);
        ps.setString(col++, partScore1);
        ps.setString(col++, partScore2);
        ps.setString(col++, partScore3);
        setStringOrNull(ps, col++, partScore4);
        ps.setString(col++, trafficPartitionTime);
        ps.setString(col, estimateBirthday);
    }

    // ── 辅助方法 ───────────────────────────────────────────────────────────────

    /** 计算字符串的 MD5 十六进制值（小写）。 */
    private static String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(32);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据注释说明生成 partition_score1： 0.1:0-23, 0.2-0.4:24-30, 0.5:31-32, 0.6-0.8:33-40, 0.9:41-45,
     * 1:46+
     */
    private static String partitionScore1(Random rnd) {
        int age = rnd.nextInt(60) + 18;
        if (age <= 23) return "0.1";
        if (age <= 30) return String.format("%.1f", 0.2 + rnd.nextInt(3) * 0.1);
        if (age <= 32) return "0.5";
        if (age <= 40) return String.format("%.1f", 0.6 + rnd.nextInt(3) * 0.1);
        if (age <= 45) return "0.9";
        return "1";
    }

    private static void setDateTimeOrNull(PreparedStatement ps, int idx, LocalDateTime dt)
            throws SQLException {
        if (dt == null) {
            ps.setNull(idx, java.sql.Types.TIMESTAMP);
        } else {
            ps.setString(idx, dt.format(DT_FMT));
        }
    }

    private static void setStringOrNull(PreparedStatement ps, int idx, String val)
            throws SQLException {
        if (val == null) {
            ps.setNull(idx, java.sql.Types.VARCHAR);
        } else {
            ps.setString(idx, val);
        }
    }
}
