package org.apache.flink.connector.jdbc.plus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 数据生成工具类：在 db_sync 库下创建宽表 t_wide_order，覆盖常见 MySQL 类型， 并批量插入随机测试数据。
 *
 * <p>表结构（20 个字段）：
 *
 * <ul>
 *   <li>id BIGINT — 主键，非自增，从 [1, 1_000_000] 随机抽取，跨度大
 *   <li>order_no VARCHAR(32)
 *   <li>user_id BIGINT
 *   <li>status TINYINT (0~4)
 *   <li>amount DECIMAL(12,2)
 *   <li>discount FLOAT
 *   <li>score DOUBLE
 *   <li>is_deleted BIT(1)
 *   <li>remark TEXT
 *   <li>category CHAR(4)
 *   <li>tags VARCHAR(200)
 *   <li>quantity INT
 *   <li>weight DECIMAL(8,3)
 *   <li>product_id BIGINT
 *   <li>region_code SMALLINT
 *   <li>priority TINYINT UNSIGNED
 *   <li>extra_json JSON
 *   <li>order_date DATE
 *   <li>pay_time DATETIME(3)
 *   <li>updated_at TIMESTAMP(3)
 * </ul>
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn test-compile
 *   mvn exec:java -Dexec.mainClass="org.apache.flink.connector.jdbc.plus.DataGenDemo" \
 *                 -Dexec.classpathScope="test"
 * </pre>
 */
public class DataGenDemo {

    // ── 连接信息（与 JdbcPlusDemo 保持一致）─────────────────────────────────────
    static final String JDBC_URL =
            "jdbc:mysql://localhost:13306/db_sync"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    static final String USERNAME = "root";
    static final String PASSWORD = "123456";

    /** 每次生成的行数，可按需调整 */
    static final int ROW_COUNT = 10000;

    /** 批量提交大小 */
    static final int BATCH_SIZE = 50;

    // ── DDL ──────────────────────────────────────────────────────────────────
    static final String DDL =
            ""
                    + "CREATE TABLE IF NOT EXISTS t_wide_order (\n"
                    + "    id           BIGINT         NOT NULL COMMENT '主键，非自增，跨度大的随机值',\n"
                    + "    order_no     VARCHAR(32)    NOT NULL COMMENT '订单编号',\n"
                    + "    user_id      BIGINT         NOT NULL COMMENT '用户ID',\n"
                    + "    status       TINYINT        NOT NULL DEFAULT 0 COMMENT '状态 0待支付 1已支付 2已发货 3已完成 4已取消',\n"
                    + "    amount       DECIMAL(12, 2) NOT NULL COMMENT '订单金额',\n"
                    + "    discount     FLOAT          NOT NULL DEFAULT 0 COMMENT '折扣率',\n"
                    + "    score        DOUBLE         NOT NULL DEFAULT 0 COMMENT '评分',\n"
                    + "    is_deleted   BIT(1)         NOT NULL DEFAULT b'0' COMMENT '逻辑删除',\n"
                    + "    remark       TEXT                    COMMENT '备注',\n"
                    + "    category     CHAR(4)        NOT NULL COMMENT '品类码',\n"
                    + "    tags         VARCHAR(200)            COMMENT '标签，逗号分隔',\n"
                    + "    quantity     INT            NOT NULL DEFAULT 1 COMMENT '数量',\n"
                    + "    weight       DECIMAL(8, 3)  NOT NULL DEFAULT 0 COMMENT '重量(kg)',\n"
                    + "    product_id   BIGINT         NOT NULL COMMENT '商品ID',\n"
                    + "    region_code  SMALLINT       NOT NULL COMMENT '区域编码',\n"
                    + "    priority     TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '优先级 0~255',\n"
                    + "    extra_json   JSON                    COMMENT '扩展JSON',\n"
                    + "    order_date   DATE           NOT NULL COMMENT '下单日期',\n"
                    + "    pay_time     DATETIME(3)             COMMENT '支付时间',\n"
                    + "    updated_at   TIMESTAMP(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n"
                    + "    PRIMARY KEY (id)\n"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='宽表测试'";

    // ── main ─────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            createTable(conn);
            List<Long> ids = generateIds(ROW_COUNT);
            insertRows(conn, ids);
            System.out.printf("✓ 已向 t_wide_order 插入 %d 行数据%n", ids.size());
        }
    }

    // ── 建表 ─────────────────────────────────────────────────────────────────

    /** 执行建表 DDL（IF NOT EXISTS，幂等）。 */
    public static void createTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(DDL);
            System.out.println("✓ 表 t_wide_order 已就绪");
        }
    }

    // ── ID 生成：从 [1, 1_000_000] 中随机抽取 rowCount 个不重复的值 ──────────

    /**
     * 生成 {@code count} 个分布在 [1, 1_000_000] 内的不重复随机 BIGINT 主键。 使用 Fisher-Yates partial shuffle
     * 保证唯一性且 O(count) 时间复杂度。
     */
    public static List<Long> generateIds(int count) {
        if (count > 1_000_000) {
            throw new IllegalArgumentException("count 不能超过 1_000_000");
        }
        Random rnd = new Random();
        // 用 List 存储 [1, 1_000_000] 并做部分 shuffle
        List<Long> pool = new ArrayList<>(count);
        for (int i = 1; i <= 1_000_000; i++) {
            pool.add((long) i);
        }
        // Fisher-Yates：只打乱前 count 个位置
        for (int i = 0; i < count; i++) {
            int j = i + rnd.nextInt(1_000_000 - i);
            Collections.swap(pool, i, j);
        }
        return pool.subList(0, count);
    }

    // ── 数据插入 ──────────────────────────────────────────────────────────────

    private static final String INSERT_SQL =
            ""
                    + "INSERT INTO t_wide_order "
                    + "(id, order_no, user_id, status, amount, discount, score, is_deleted, remark, "
                    + " category, tags, quantity, weight, product_id, region_code, priority, extra_json, "
                    + " order_date, pay_time, updated_at) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String[] CATEGORIES = {"ELEC", "FOOD", "BOOK", "CLTH", "SPTS"};
    private static final String[] TAG_POOL = {"热销", "新品", "打折", "限时", "爆款", "推荐", "清仓", "预售"};
    private static final String[] REGIONS = {"BJ", "SH", "GZ", "SZ", "CD", "HZ", "WH", "XA"};

    /** 批量插入随机行，以 {@link #BATCH_SIZE} 为单位提交事务。 */
    public static void insertRows(Connection conn, List<Long> ids) throws SQLException {
        Random rnd = new Random();
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);

        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            for (int i = 0; i < ids.size(); i++) {
                long id = ids.get(i);
                setRow(ps, id, rnd);
                ps.addBatch();

                if ((i + 1) % BATCH_SIZE == 0 || i == ids.size() - 1) {
                    ps.executeBatch();
                    conn.commit();
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    /** 为单行随机填充所有字段。 */
    private static void setRow(PreparedStatement ps, long id, Random rnd) throws SQLException {
        LocalDateTime now = LocalDateTime.now();
        // 下单日期：近两年内随机
        LocalDate orderDate = LocalDate.now().minusDays(rnd.nextInt(730));
        // 支付时间：下单日期后 0~30 分钟，20% 概率为 NULL（未支付）
        LocalDateTime payTime =
                rnd.nextInt(5) == 0
                        ? null
                        : orderDate.atTime(rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60));

        ps.setLong(1, id);
        ps.setString(2, String.format("ORD%016d", id * 7 + rnd.nextInt(9999)));
        ps.setLong(3, (long) (rnd.nextInt(100_000) + 1));
        ps.setByte(4, (byte) rnd.nextInt(5));
        ps.setBigDecimal(
                5, new java.math.BigDecimal(String.format("%.2f", rnd.nextDouble() * 9999 + 0.01)));
        ps.setFloat(6, Math.round(rnd.nextFloat() * 100f) / 100f);
        ps.setDouble(7, Math.round(rnd.nextDouble() * 500d * 100d) / 100d);
        ps.setBoolean(8, rnd.nextInt(10) == 0); // 10% 逻辑删除
        ps.setString(9, rnd.nextInt(3) == 0 ? null : randomRemark(rnd));
        ps.setString(10, CATEGORIES[rnd.nextInt(CATEGORIES.length)]);
        ps.setString(11, randomTags(rnd));
        ps.setInt(12, rnd.nextInt(20) + 1);
        ps.setBigDecimal(
                13, new java.math.BigDecimal(String.format("%.3f", rnd.nextDouble() * 50)));
        ps.setLong(14, (long) (rnd.nextInt(50_000) + 1));
        ps.setShort(15, (short) (rnd.nextInt(500) + 1));
        ps.setShort(16, (short) rnd.nextInt(256));
        ps.setString(
                17,
                String.format(
                        "{\"source\":\"%s\",\"version\":%d,\"tags\":[%s]}",
                        REGIONS[rnd.nextInt(REGIONS.length)], rnd.nextInt(10) + 1, "\"a\",\"b\""));
        ps.setObject(18, orderDate);
        if (payTime == null) {
            ps.setNull(19, java.sql.Types.TIMESTAMP);
        } else {
            ps.setObject(19, payTime);
        }
        ps.setObject(20, now);
    }

    private static String randomRemark(Random rnd) {
        String[] remarks = {
            "正常订单", "客户催发货", "需要开发票", "礼品包装", "尽快处理", "备注稍后补充", "已电话确认", "优先配送", "节假日订单"
        };
        return remarks[rnd.nextInt(remarks.length)];
    }

    private static String randomTags(Random rnd) {
        int count = rnd.nextInt(3) + 1;
        List<String> picked = new ArrayList<>();
        List<String> pool = new ArrayList<>(Arrays.asList(TAG_POOL));
        Collections.shuffle(pool, rnd);
        for (int i = 0; i < count && i < pool.size(); i++) {
            picked.add(pool.get(i));
        }
        return String.join(",", picked);
    }
}
