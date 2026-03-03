# flink-jdbc-plus

An enhanced Flink JDBC source connector that fixes three major pain-points of the
native `flink-connector-jdbc`:

| Pain-point | Native JDBC connector | flink-jdbc-plus |
|---|---|---|
| Split key skew / non-uniform data | Arithmetic `[min,max]/N` splits → empty or huge chunks | **Non-uniform chunk splitting**: queries actual N-th row boundaries |
| Max / min must be set manually | Requires `scan.partition.lower-bound` + `upper-bound` | **Fully automatic** min/max detection |
| Single table only | One `CREATE TABLE` per physical table | **Multi-table**: explicit list or Java regex |
| JM/TM coupling | Enumerator and reader share mutable state | **FLIP-27 clean**: pure message-passing between JM and TM |

---

## Architecture

```
JobManager (JdbcSourceEnumerator)
  │
  │  ① start() → callAsync(discoverAllSplits)   ← heavy JDBC work on background thread
  │  ② onSplitsDiscovered(splits) callback       ← runs on JM main thread (thread-safe)
  │  ③ handleSplitRequest(subtaskId)             ← context.assignSplit(split, subtaskId)
  │  ④ checkpoint: snapshotState()               ← serialise pendingSplits + remainingTables
  │
  │  [FLIP-27 split-assignment messages only — no shared state with TM]
  │
TaskManager (JdbcSourceReader — one per subtask)
  │
  ├── SplitFetcherManager
  │     └── Background (producer) thread: JdbcSplitReader.fetch()
  │           └── Opens ResultSet for each split, wraps in JdbcResultSetRecords
  │           └── Places RecordsWithSplitIds into FutureCompletingBlockingQueue
  │
  └── Main (consumer) thread: SourceReaderBase.pollNext()
        └── Dequeues batches, calls JdbcRecordEmitter.emitRecord()
              → output.collect(rowData) + splitState.incrementOffset()
```

---

## Non-Uniform Chunk Splitting

Inspired by the Debezium / Flink CDC `ChunkSplitter`.

### The problem with arithmetic splitting

The native connector computes N chunk boundaries as:

```
boundary_i = min + i * (max - min) / N
```

If your `id` column has values like `{1, 2, 3, 10_000_000, 10_000_001}` and you ask for
5 chunks you get four empty chunks and one chunk with all the data.

### The solution: data-driven boundaries

flink-jdbc-plus queries the *actual N-th row*:

```sql
-- find the boundary after `chunkSize` rows from current start
SELECT `id` FROM `mydb`.`orders`
WHERE  `id` > ?            -- omitted for the first chunk
ORDER BY `id`
LIMIT 8096, 1              -- skip 8096 rows, return the 8097th
```

If the query returns `b`, the current chunk is `(start, b]` with exactly `chunkSize` rows.
Repeat until the query returns nothing → final chunk is `(last_b, ∞)`.

**Works for any orderable SQL type** — integers, strings, dates, timestamps — without
any arithmetic. No need to set min/max manually.

---

## Multi-Table Support

### Explicit list

```properties
table-list = orders, orders_archive, order_items
```

Each entry is either a bare `tableName` (uses the configured `database`) or a
fully-qualified `database.tableName`.

### Java regex pattern

```properties
table-pattern = mydb\\.order_.*
```

The regex is matched against every `database.tableName` returned by
`information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`.

---

## Quick Start

### DataStream API

```java
JdbcPlusSource source = JdbcPlusSource.builder()
    .url("jdbc:mysql://localhost:3306/mydb")
    .username("root")
    .password("secret")
    .database("mydb")
    // Option A: explicit list
    .tableList("orders", "order_items")
    // Option B: regex (uncomment and remove tableList)
    // .tablePattern("mydb\\.order.*")
    .chunkSize(8096)
    .rowType(rowType)      // Flink RowType matching the table schema
    .build();

DataStream<RowData> stream = env.fromSource(
    source, WatermarkStrategy.noWatermarks(), "jdbc-plus-source");
```

### Table / SQL API

```sql
CREATE TABLE orders_source (
  id       BIGINT,
  amount   DECIMAL(10, 2),
  status   VARCHAR(32),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector'     = 'jdbc-plus',
  'url'           = 'jdbc:mysql://localhost:3306/mydb',
  'username'      = 'root',
  'password'      = 'secret',
  'database'      = 'mydb',
  'table-list'    = 'orders,orders_archive',
  'chunk-size'    = '8096',
  'fetch-size'    = '1024'
);

INSERT INTO sink SELECT * FROM orders_source;
```

---

## Configuration Reference

| Option | Required | Default | Description |
|---|---|---|---|
| `url` | ✅ | — | JDBC connection URL |
| `database` | ✅ | — | Default database/schema |
| `table-list` | ⚠️ one of | — | Comma-separated table names |
| `table-pattern` | ⚠️ one of | — | Java regex on `db.table` |
| `username` | | — | JDBC username |
| `password` | | — | JDBC password |
| `driver` | | auto | JDBC driver class name |
| `split-key-column` | | auto (PK) | Column for chunk splitting |
| `chunk-size` | | `8096` | Target rows per chunk |
| `fetch-size` | | `1024` | JDBC fetch size |
| `connection-timeout-ms` | | `30000` | Connection timeout |
| `max-retries` | | `3` | Retry count on errors |
| `dialect` | | `mysql` | SQL dialect |

---

## Project Structure

```
src/main/java/org/apache/flink/connector/jdbc/plus/
├── JdbcPlusSource.java                    # Top-level FLIP-27 Source<RowData,...>
├── JdbcPlusOptions.java                   # Immutable configuration
├── split/
│   ├── JdbcSourceSplit.java               # Chunk: (table, splitKey, start, end, offset)
│   ├── JdbcSourceSplitState.java          # Mutable per-reader split state
│   └── JdbcSourceSplitSerializer.java     # Checkpoint serialisation
├── enumerator/
│   ├── JdbcSourceEnumerator.java          # JM-side: discovers + assigns splits
│   ├── JdbcSourceEnumeratorState.java     # JM checkpoint state
│   └── JdbcSourceEnumeratorStateSerializer.java
├── reader/
│   ├── JdbcSourceReader.java              # TM-side: SourceReaderBase subclass
│   ├── JdbcSplitReader.java               # TM low-level: opens ResultSet per split
│   ├── JdbcResultSetRecords.java          # RecordsWithSplitIds wrapping ResultSet
│   ├── JdbcRecordEmitter.java             # Emits RowData + increments offset
│   └── JdbcRowConverter.java              # ResultSet → RowData conversion
├── splitter/
│   └── NonUniformChunkSplitter.java       # Key algorithm: data-driven boundaries
├── table/
│   ├── TableDiscovery.java                # List / regex table discovery
│   └── TableInfo.java                     # Table metadata (name, splitKey, type)
├── dialect/
│   ├── JdbcDialect.java                   # SQL abstraction interface
│   └── MySqlDialect.java                  # MySQL / MariaDB implementation
└── factory/
    ├── JdbcPlusDynamicTableFactory.java   # Flink Table API factory (SPI)
    └── JdbcPlusDynamicTableSource.java    # ScanTableSource wrapper
```

---

## Building

```bash
mvn clean package -DskipTests
# shaded jar: target/flink-jdbc-plus-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## Compatibility

| Component | Version |
|---|---|
| Apache Flink | 1.17.x |
| Java | 11+ |
| MySQL | 5.7 / 8.x |
| MariaDB | 10.x |

To support PostgreSQL, Oracle, or SQL Server, implement the `JdbcDialect` interface
and pass your implementation via `JdbcPlusSource.builder().dialect(myDialect)`.
