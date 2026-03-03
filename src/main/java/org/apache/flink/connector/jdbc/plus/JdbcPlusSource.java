package org.apache.flink.connector.jdbc.plus;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.jdbc.plus.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.plus.dialect.MySqlDialect;
import org.apache.flink.connector.jdbc.plus.enumerator.JdbcSourceEnumerator;
import org.apache.flink.connector.jdbc.plus.enumerator.JdbcSourceEnumeratorState;
import org.apache.flink.connector.jdbc.plus.enumerator.JdbcSourceEnumeratorStateSerializer;
import org.apache.flink.connector.jdbc.plus.reader.JdbcSourceReader;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.plus.split.JdbcSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Objects;

/**
 * Top-level FLIP-27 {@link Source} for reading from JDBC databases with:
 *
 * <ul>
 *   <li><b>Non-uniform chunk splitting</b>: chunk boundaries are discovered by querying actual data
 *       distribution, so every chunk has approximately {@code chunkSize} rows regardless of gaps or
 *       skew in the split key column.
 *   <li><b>Multi-table support</b>: specify tables as an explicit list or a Java regex; the
 *       connector discovers and reads all matching tables in parallel.
 *   <li><b>Clean JM/TM decoupling</b>: the {@link JdbcSourceEnumerator} runs on the JobManager and
 *       communicates split assignments only through FLIP-27 messages; the {@link JdbcSourceReader}
 *       runs on TaskManagers and never shares state with the enumerator.
 * </ul>
 *
 * <h2>Usage (DataStream API)</h2>
 *
 * <pre>{@code
 * JdbcPlusSource source = JdbcPlusSource.builder()
 *     .url("jdbc:mysql://localhost:3306/mydb")
 *     .username("root").password("secret")
 *     .database("mydb")
 *     .tableList("orders", "order_items")   // explicit list
 *     // OR: .tablePattern("mydb\\.order.*") // regex
 *     .chunkSize(8096)
 *     .rowType(rowType)
 *     .build();
 *
 * DataStream<RowData> stream = env.fromSource(
 *     source, WatermarkStrategy.noWatermarks(), "jdbc-plus");
 * }</pre>
 *
 * <h2>Usage (Table API)</h2>
 *
 * <p>Register the factory in META-INF/services and use the {@code jdbc-plus} connector identifier
 * in CREATE TABLE DDL.
 */
public class JdbcPlusSource
        implements Source<RowData, JdbcSourceSplit, JdbcSourceEnumeratorState>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final JdbcPlusOptions options;
    private final JdbcDialect dialect;
    private final RowType rowType;
    private final TypeInformation<RowData> producedTypeInfo;

    private JdbcPlusSource(
            JdbcPlusOptions options,
            JdbcDialect dialect,
            RowType rowType,
            TypeInformation<RowData> producedTypeInfo) {
        this.options = Objects.requireNonNull(options, "options");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.rowType = Objects.requireNonNull(rowType, "rowType");
        this.producedTypeInfo = Objects.requireNonNull(producedTypeInfo, "producedTypeInfo");
    }

    // -------------------------------------------------------------------------
    // Source interface
    // -------------------------------------------------------------------------

    /** This is a bounded (batch) source — it reads a fixed snapshot of the database. */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    /** Creates a new {@link JdbcSourceReader} for a TaskManager subtask. */
    @Override
    public SourceReader<RowData, JdbcSourceSplit> createReader(SourceReaderContext context) {
        return new JdbcSourceReader(context, options, dialect, rowType);
    }

    /**
     * Creates a new {@link JdbcSourceEnumerator} for a fresh job start. Discovery and chunk
     * splitting are triggered inside the enumerator's {@code start()}.
     */
    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context) {
        return new JdbcSourceEnumerator(context, options, dialect);
    }

    /**
     * Restores the enumerator from a checkpoint, preserving pending splits and the list of tables
     * that were not yet fully split at the time of the snapshot.
     */
    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context, JdbcSourceEnumeratorState checkpoint) {
        return new JdbcSourceEnumerator(context, options, dialect, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceSplit> getSplitSerializer() {
        return new JdbcSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new JdbcSourceEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private JdbcPlusOptions options;
        private JdbcDialect dialect = MySqlDialect.INSTANCE;
        private RowType rowType;
        private TypeInformation<RowData> producedTypeInfo;

        // ── Shortcut setters that forward to JdbcPlusOptions.Builder ──

        private JdbcPlusOptions.Builder optBuilder = JdbcPlusOptions.builder();

        public Builder url(String url) {
            optBuilder.url(url);
            return this;
        }

        public Builder username(String u) {
            optBuilder.username(u);
            return this;
        }

        public Builder password(String p) {
            optBuilder.password(p);
            return this;
        }

        public Builder driverName(String d) {
            optBuilder.driverName(d);
            return this;
        }

        public Builder database(String db) {
            optBuilder.database(db);
            return this;
        }

        public Builder tableList(String... t) {
            optBuilder.tableList(t);
            return this;
        }

        public Builder tableList(List<String> t) {
            optBuilder.tableList(t);
            return this;
        }

        public Builder tablePattern(String p) {
            optBuilder.tablePattern(p);
            return this;
        }

        public Builder splitKeyColumn(String c) {
            optBuilder.splitKeyColumn(c);
            return this;
        }

        public Builder chunkSize(int n) {
            optBuilder.chunkSize(n);
            return this;
        }

        public Builder fetchSize(int n) {
            optBuilder.fetchSize(n);
            return this;
        }

        public Builder connectionTimeoutMs(int t) {
            optBuilder.connectionTimeoutMs(t);
            return this;
        }

        public Builder maxRetries(int n) {
            optBuilder.maxRetries(n);
            return this;
        }

        /** Provide a fully-built {@link JdbcPlusOptions} instead of individual setters. */
        public Builder options(JdbcPlusOptions options) {
            this.options = options;
            return this;
        }

        /** Override the SQL dialect. Defaults to {@link MySqlDialect}. */
        public Builder dialect(JdbcDialect dialect) {
            this.dialect = Objects.requireNonNull(dialect);
            return this;
        }

        /**
         * Required: the Flink {@link RowType} that describes the output schema. Column order must
         * match the {@code SELECT *} order of the target table.
         */
        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        /** Optional: set the TypeInformation explicitly (auto-derived from rowType if omitted). */
        public Builder producedTypeInfo(TypeInformation<RowData> typeInfo) {
            this.producedTypeInfo = typeInfo;
            return this;
        }

        public JdbcPlusSource build() {
            JdbcPlusOptions resolvedOptions = (options != null) ? options : optBuilder.build();
            Objects.requireNonNull(rowType, "rowType must be set");
            TypeInformation<RowData> resolvedTypeInfo =
                    (producedTypeInfo != null)
                            ? producedTypeInfo
                            : org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(rowType);
            return new JdbcPlusSource(resolvedOptions, dialect, rowType, resolvedTypeInfo);
        }
    }
}
