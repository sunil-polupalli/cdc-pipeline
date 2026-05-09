# Fault-Tolerant CDC Pipeline with Schema Evolution

A production-grade **Change Data Capture (CDC)** pipeline that streams data from MySQL, handles schema evolution automatically, and writes partitioned Apache Parquet files to a local data lake — all orchestrated with a single `docker-compose up`.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
  - [1. MySQL Source & Seeding](#1-mysql-source--seeding)
  - [2. Debezium CDC Source](#2-debezium-cdc-source)
  - [3. Schema Registry](#3-schema-registry)
  - [4. Pipeline Processor](#4-pipeline-processor)
  - [5. Parquet Data Lake](#5-parquet-data-lake)
  - [6. Lineage Report](#6-lineage-report)
- [Simulating Schema Evolution](#simulating-schema-evolution)
- [Verifying the Output](#verifying-the-output)
- [Design Decisions](#design-decisions)
- [Fault Tolerance & At-Least-Once Guarantees](#fault-tolerance--at-least-once-guarantees)
- [Environment Variables Reference](#environment-variables-reference)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────┐    binlog     ┌───────────┐   change events   ┌─────────┐
│   MySQL 8.0  │ ──────────── │  Debezium  │ ────────────────► │  Kafka  │
│  (source DB) │   (GTID/ROW) │  Connect  │                   │ broker  │
└─────────────┘              └───────────┘                   └────┬────┘
                                                                   │
                                                          ┌────────▼────────┐
                                                          │    Processor     │
                                                          │  (Python app)    │
                                                          │                  │
                                                          │  ┌────────────┐  │
                                                          │  │  Schema    │  │
                                                          │  │ Registry   │  │
                                                          │  │ (SQLite)   │  │
                                                          │  └────────────┘  │
                                                          └────────┬─────────┘
                                                                   │ Parquet
                                                          ┌────────▼─────────┐
                                                          │    Data Lake      │
                                                          │  ./data_lake/     │
                                                          │  {table}/         │
                                                          │   {date}/         │
                                                          │    {op}/          │
                                                          │     *.parquet     │
                                                          └──────────────────┘
```

**Services (all containerised):**

| Service | Image | Role |
|---|---|---|
| `mysql` | `mysql:8.0` | Source of truth; binlog-enabled with GTID |
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | Kafka co-ordinator |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | Durable change-event log |
| `connect` | `debezium/connect:2.5` | Reads MySQL binlog, publishes to Kafka |
| `processor` | *(built locally)* | Consumes Kafka, manages schemas, writes Parquet |

---

## Key Features

- **Zero-downtime schema evolution** — column renames and additions are detected automatically; new schema versions are registered and Parquet output is updated without restarting the pipeline.
- **SQLite schema registry** — lightweight, file-based, persisted via Docker volume. Each table has a versioned history of every schema it has ever had.
- **Partitioned Parquet data lake** — output lives at `./data_lake/{table}/{YYYY-MM-DD}/{op_type}/`, compatible with DuckDB, Spark, Athena, etc.
- **At-least-once delivery** — Kafka offsets are committed only *after* data is flushed to disk, so a crash never loses data.
- **Data lineage report** — `output/lineage_report.json` is regenerated every 60 seconds and tracks schema transitions, active time windows, and output partitions.
- **One-command setup** — `docker-compose up` starts and health-checks every service in the correct order.

---

## Project Structure

```
cdc-pipeline/
├── docker-compose.yml          # Full environment definition
├── .env.example                # All required env vars (copy → .env)
├── .gitignore
├── README.md
│
├── mysql/
│   ├── my.cnf                  # Binlog + GTID configuration
│   └── init/
│       ├── 01-schema.sql       # CREATE TABLE statements
│       ├── 02-data.sql         # 510,000 seed rows (auto-generated)
│       └── 03-users.sql        # Debezium replication user
│
├── processor/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # Entry point; Kafka consumer loop
│   ├── schema_registry.py      # SQLite-backed schema versioning
│   ├── parquet_writer.py       # Buffered Parquet flush to data lake
│   ├── register_connector.py   # Debezium connector REST registration
│   ├── lineage_report.py       # Lineage report generator
│   └── generate_lineage_report.py  # CLI wrapper for on-demand report
│
├── scripts/
│   ├── generate_data.py        # Generates mysql/init/02-data.sql
│   └── simulate_schema_evolution.sh  # Applies ALTER TABLE statements
│
├── output/
│   └── lineage_report.json     # Committed baseline report
│
├── data_lake/                  # Runtime output (git-ignored)
└── state/                      # SQLite schemas.db (git-ignored)
```

---

## Prerequisites

- **Docker** ≥ 20.10
- **Docker Compose** ≥ 1.29 (or `docker compose` v2 plugin)
- ~4 GB free RAM (Kafka + Debezium are memory-hungry)
- ~2 GB free disk for the seed data and Parquet output

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone "https://github.com/sunil-polupalli/cdc-pipeline.git" && cd cdc-pipeline

# 2. Set up environment variables
cp .env.example .env
# The defaults in .env.example work out of the box.

# 3. Start everything (takes ~2–3 minutes for MySQL seeding)
docker-compose up -d

# 4. Watch the processor logs
docker logs -f cdc-processor

# 5. (Optional) Simulate schema evolution
./scripts/simulate_schema_evolution.sh

# 6. Regenerate lineage report on demand
docker exec cdc-processor python generate_lineage_report.py
```

After a few minutes you will see Parquet files appearing under `./data_lake/`.

---

## How It Works

### 1. MySQL Source & Seeding

MySQL is configured via `mysql/my.cnf` to enable **row-based binary logging** with **GTID consistency**, which Debezium requires to reconstruct a reliable change stream:

```ini
[mysqld]
server-id        = 1
log-bin          = mysql-bin
binlog_format    = ROW
gtid_mode        = ON
enforce_gtid_consistency = ON
binlog_row_image = FULL
```

On first startup the Docker entrypoint runs three SQL scripts in order:

| Script | Purpose |
|---|---|
| `01-schema.sql` | Creates `customers`, `products`, `orders` tables |
| `02-data.sql` | Inserts **510,000 rows** (100k customers, 10k products, 400k orders) |
| `03-users.sql` | Creates `debezium_user` with `REPLICATION SLAVE` privilege |

The seed data was generated by `scripts/generate_data.py`, which produces batched `INSERT` statements to keep the file manageable.

### 2. Debezium CDC Source

Once MySQL and Kafka are healthy, the **processor** automatically registers the Debezium MySQL connector via the Kafka Connect REST API (`POST /connectors`). The connector configuration:

- Monitors all three tables in the `inventory` database.
- Uses `snapshot.mode=initial` so every existing row produces a `create` event on first run.
- Publishes events to topics: `dbserver1.inventory.{customers|products|orders}`.
- Uses `JsonConverter` so messages are human-readable JSON.

Each Debezium event contains:
```json
{
  "payload": {
    "before": { ... },     // null for inserts
    "after":  { ... },     // null for deletes
    "op": "c|u|d|r",       // create, update, delete, snapshot-read
    "ts_ms": 1234567890000
  },
  "schema": { ... }        // full field-type metadata
}
```

### 3. Schema Registry

`schema_registry.py` maintains a SQLite database at `/state/schemas.db` (mounted to `./state/` on the host).

**Schema detection logic:**

1. Extract column → type mapping from the Debezium message's `schema.fields` block.
2. Serialise to a canonical, sorted JSON string.
3. Query `table_schemas` for an identical `schema_definition`.
4. If found → return existing `schema_version`.
5. If not found → `version = max_version + 1`, close the previous version's `active_to`, insert new row.

This gives you a complete, queryable audit trail:

```
table_name  | schema_version | created_at           | active_to
------------|----------------|----------------------|---------------------
products    | 1              | 2025-01-01T10:00:00Z | 2025-01-01T10:10:00Z
products    | 2              | 2025-01-01T10:10:00Z | NULL
customers   | 1              | 2025-01-01T10:00:00Z | 2025-01-01T10:15:00Z
customers   | 2              | 2025-01-01T10:15:00Z | NULL
```

### 4. Pipeline Processor

`main.py` runs a tight Kafka consumer loop:

```
for each poll batch:
  for each message:
    → parse Debezium envelope
    → detect operation type (c/u/d)
    → extract data payload (after/before)
    → derive schema, call schema_registry
    → enrich record with _op_type, _event_timestamp, _schema_version
    → buffer_record(table, date, op, enriched_record)
  commit Kafka offsets   ← only AFTER processing
```

Flushing is triggered by **either**:
- Buffer reaches `FLUSH_BATCH_SIZE` (default 200) records, **or**
- `FLUSH_INTERVAL_SEC` (default 30 s) has elapsed since the last flush.

### 5. Parquet Data Lake

`parquet_writer.py` builds PyArrow tables column-by-column, inferring types from Python native types. It handles **schema drift within a partition** by collecting all column names across a batch (superset) and padding missing values with `None`.

Output layout:
```
data_lake/
├── customers/
│   └── 2025-01-01/
│       ├── c/  part_000001_1704067200.parquet
│       └── u/  part_000001_1704067230.parquet
├── products/
│   └── 2025-01-01/
│       ├── c/  part_000001_1704067200.parquet   ← schema v1
│       └── c/  part_000002_1704067800.parquet   ← schema v2 (product_description)
└── orders/
    └── 2025-01-01/
        └── c/  part_000001_1704067200.parquet
```

You can query the lake instantly with DuckDB:
```sql
SELECT * FROM read_parquet('data_lake/products/**/*.parquet');
```

### 6. Lineage Report

`lineage_report.py` is run in a background thread every 60 seconds. It:

1. Reads all schema records from SQLite.
2. Walks the data lake directory tree to find which partitions exist.
3. Writes `output/lineage_report.json`.

---

## Simulating Schema Evolution

With the pipeline running:

```bash
./scripts/simulate_schema_evolution.sh
```

This applies two ALTER TABLE statements:

**Change 1 – Column rename:**
```sql
ALTER TABLE products RENAME COLUMN description TO product_description;
```
→ Pipeline detects new schema, registers `products` **v2**, subsequent Parquet files contain `product_description`.

**Change 2 – Add NOT NULL column:**
```sql
ALTER TABLE customers ADD COLUMN country_code VARCHAR(3) NOT NULL DEFAULT 'USA';
```
→ Pipeline detects new schema, registers `customers` **v2**, Parquet files gain `country_code`.

**The pipeline never crashes or loses data during either change.**

---

## Verifying the Output

```bash
# Check schema registry
sqlite3 state/schemas.db "SELECT table_name, schema_version, created_at FROM table_schemas;"

# List Parquet files
find data_lake -name "*.parquet" | sort

# Inspect a Parquet file with Python
python3 - <<'EOF'
import pyarrow.parquet as pq, glob
files = glob.glob("data_lake/**/*.parquet", recursive=True)
for f in files[:3]:
    t = pq.read_table(f)
    print(f"\n{f}")
    print("  schema:", t.schema.names)
    print("  rows:  ", t.num_rows)
EOF

# View lineage report
cat output/lineage_report.json | python3 -m json.tool
```

---

## Design Decisions

### Why SQLite for the schema registry?
SQLite is a perfect fit for this use case: it's embedded (no extra service), ACID-compliant, supports SQL queries, and is trivially persisted via a Docker volume. For a distributed deployment, this would be replaced by PostgreSQL or a dedicated registry like Confluent Schema Registry — but SQLite captures the same semantics while keeping the project self-contained.

### Why file-based buffering instead of streaming Parquet writes?
Parquet is a columnar format; it needs to know all values for a column before encoding it efficiently. Buffering N records before writing gives the writer a complete column picture, enabling better compression. The flush triggers (size or time) balance latency against file count.

### Why at-least-once instead of exactly-once?
Exactly-once in Kafka requires transactional producers and idempotent consumers across the entire pipeline. For a data lake sink (append-only Parquet files), at-least-once with idempotent writes (content-addressable filenames) is simpler and equally correct — duplicate records in a batch are filtered at query time with `DISTINCT` or deduplication logic.

### Why Debezium 2.5 / Confluent 7.5?
These are the latest stable versions at time of writing with strong compatibility. Debezium 2.x moved from the older `database.history.*` config to `schema.history.internal.*`, and this project uses the new config to avoid deprecation warnings.

---

## Fault Tolerance & At-Least-Once Guarantees

| Failure Scenario | Behaviour |
|---|---|
| Processor crashes mid-batch | Restarts from last committed Kafka offset; re-processes the batch |
| MySQL restart | Debezium reconnects via binlog position stored in Kafka topic |
| Kafka restart | Producer and consumer reconnect automatically |
| Schema change during processing | New schema version registered; previous Parquet files unchanged |
| Partial Parquet write | PyArrow writes atomically to a temp path then renames |

---

## Environment Variables Reference

See `.env.example` for the full list. Key variables:

| Variable | Default | Description |
|---|---|---|
| `MYSQL_ROOT_PASSWORD` | `rootpassword` | MySQL root password |
| `MYSQL_DATABASE` | `inventory` | Database name |
| `DEBEZIUM_USER` | `debezium_user` | Replication user |
| `DEBEZIUM_PASSWORD` | `debezium_pw` | Replication user password |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap address |
| `FLUSH_BATCH_SIZE` | `200` | Records before Parquet flush |
| `FLUSH_INTERVAL_SEC` | `30` | Seconds between forced flushes |
| `LINEAGE_REPORT_INTERVAL_SEC` | `60` | Lineage report regeneration interval |

---

## Troubleshooting

**Containers don't start in order / exit with errors:**
```bash
docker-compose logs mysql    # Check if seeding is still running
docker-compose logs connect  # Check Debezium connector startup
docker-compose logs processor
```

**No Parquet files appearing:**
```bash
# Check if connector was registered
curl http://localhost:8083/connectors/mysql-cdc-connector/status

# Check Kafka topics exist
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**MySQL seeding takes too long:**
The `02-data.sql` file is ~23 MB with 510k rows. On slower machines this can take 3–5 minutes. The processor has a 5-minute startup window before health checks fail — increase `start_period` in `docker-compose.yml` if needed.

**`simulate_schema_evolution.sh` fails:**
Ensure you have set `MYSQL_ROOT_PASSWORD` in your shell (or `.env`) to match what the container was started with.

```bash
export MYSQL_ROOT_PASSWORD=rootpassword
./scripts/simulate_schema_evolution.sh
```
