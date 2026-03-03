package org.apache.flink.connector.jdbc.plus.reader;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalTime;

/**
 * Converts a JDBC {@link ResultSet} row into a Flink {@link RowData} according to the provided
 * {@link RowType} schema.
 *
 * <p>Field converters are pre-built at construction time (one per column) to avoid per-row type
 * switching.
 */
public class JdbcRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    @FunctionalInterface
    interface FieldConverter extends Serializable {
        Object convert(ResultSet rs, int columnIndex) throws SQLException;
    }

    private final RowType rowType;
    private final FieldConverter[] converters;

    public JdbcRowConverter(RowType rowType) {
        this.rowType = rowType;
        this.converters = buildConverters(rowType);
    }

    /**
     * Reads the current row from {@code rs} and returns a {@link RowData}. The result is always a
     * {@link GenericRowData} with JDBC-column-to-Flink-field mapping by index (1-based JDBC →
     * 0-based Flink).
     */
    public RowData toInternal(ResultSet rs) throws SQLException {
        int arity = rowType.getFieldCount();
        GenericRowData row = new GenericRowData(arity);
        for (int i = 0; i < arity; i++) {
            Object val = converters[i].convert(rs, i + 1);
            row.setField(i, val);
        }
        return row;
    }

    // -------------------------------------------------------------------------
    // Converter construction
    // -------------------------------------------------------------------------

    private static FieldConverter[] buildConverters(RowType rowType) {
        int n = rowType.getFieldCount();
        FieldConverter[] cvts = new FieldConverter[n];
        for (int i = 0; i < n; i++) {
            cvts[i] = createConverter(rowType.getTypeAt(i));
        }
        return cvts;
    }

    private static FieldConverter createConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return (rs, idx) -> {
                    boolean v = rs.getBoolean(idx);
                    return rs.wasNull() ? null : v;
                };
            case TINYINT:
                return (rs, idx) -> {
                    byte v = rs.getByte(idx);
                    return rs.wasNull() ? null : v;
                };
            case SMALLINT:
                return (rs, idx) -> {
                    short v = rs.getShort(idx);
                    return rs.wasNull() ? null : v;
                };
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (rs, idx) -> {
                    int v = rs.getInt(idx);
                    return rs.wasNull() ? null : v;
                };
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (rs, idx) -> {
                    long v = rs.getLong(idx);
                    return rs.wasNull() ? null : v;
                };
            case FLOAT:
                return (rs, idx) -> {
                    float v = rs.getFloat(idx);
                    return rs.wasNull() ? null : v;
                };
            case DOUBLE:
                return (rs, idx) -> {
                    double v = rs.getDouble(idx);
                    return rs.wasNull() ? null : v;
                };
            case DECIMAL:
                {
                    DecimalType dt = (DecimalType) type;
                    int precision = dt.getPrecision();
                    int scale = dt.getScale();
                    return (rs, idx) -> {
                        BigDecimal bd = rs.getBigDecimal(idx);
                        return bd == null ? null : DecimalData.fromBigDecimal(bd, precision, scale);
                    };
                }
            case CHAR:
            case VARCHAR:
                return (rs, idx) -> {
                    String s = rs.getString(idx);
                    return s == null ? null : StringData.fromString(s);
                };
            case BINARY:
            case VARBINARY:
                return ResultSet::getBytes;
            case DATE:
                return (rs, idx) -> {
                    java.sql.Date d = rs.getDate(idx);
                    if (d == null) return null;
                    // Flink DATE is stored as int: days since epoch.
                    return (int) d.toLocalDate().toEpochDay();
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (rs, idx) -> {
                    java.sql.Time t = rs.getTime(idx);
                    if (t == null) return null;
                    // Flink TIME is stored as int: milliseconds of day.
                    LocalTime lt = t.toLocalTime();
                    return (int) (lt.toNanoOfDay() / 1_000_000L);
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    return (rs, idx) -> {
                        Timestamp ts = rs.getTimestamp(idx);
                        if (ts == null) return null;
                        return TimestampData.fromTimestamp(ts);
                    };
                }
            default:
                // Fallback: store as string representation.
                return (rs, idx) -> {
                    String s = rs.getString(idx);
                    return s == null ? null : StringData.fromString(s);
                };
        }
    }
}
