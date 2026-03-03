package org.apache.flink.connector.jdbc.plus.factory;

import org.apache.flink.connector.jdbc.plus.JdbcPlusOptions;
import org.apache.flink.connector.jdbc.plus.JdbcPlusSource;
import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/**
 * {@link ScanTableSource} implementation that wraps {@link JdbcPlusSource} for integration with
 * Flink's Table API and SQL.
 *
 * <p>An instance of this class is created by {@link JdbcPlusDynamicTableFactory} for each {@code
 * CREATE TABLE … WITH ('connector' = 'jdbc-plus', …)} statement.
 *
 * <h2>Multi-table note</h2>
 *
 * <p>In the Table API, a single DDL statement declares one logical table schema. When {@code
 * table-list} or {@code table-pattern} references multiple physical tables they must all share the
 * same column layout. The emitted {@code RowData} rows contain the data columns only (no implicit
 * table-name column); if the consumer needs to distinguish source tables, add a computed column or
 * use a separate source per table.
 */
public class JdbcPlusDynamicTableSource implements ScanTableSource {

    private final JdbcPlusOptions options;
    private final JdbcDialect dialect;
    private final RowType rowType;

    public JdbcPlusDynamicTableSource(
            JdbcPlusOptions options, JdbcDialect dialect, RowType rowType) {
        this.options = Objects.requireNonNull(options, "options");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.rowType = Objects.requireNonNull(rowType, "rowType");
    }

    // -------------------------------------------------------------------------
    // ScanTableSource
    // -------------------------------------------------------------------------

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        JdbcPlusSource source =
                JdbcPlusSource.builder().options(options).dialect(dialect).rowType(rowType).build();
        return SourceProvider.of(source);
    }

    // -------------------------------------------------------------------------
    // DynamicTableSource
    // -------------------------------------------------------------------------

    @Override
    public DynamicTableSource copy() {
        return new JdbcPlusDynamicTableSource(options, dialect, rowType);
    }

    @Override
    public String asSummaryString() {
        return "JdbcPlus["
                + options.getDatabase()
                + (options.useTablePattern()
                        ? "/" + options.getTablePattern()
                        : "/" + options.getTableList())
                + "]";
    }
}
