"""
parquet_writer.py
Buffers CDC records and flushes them as Parquet files into the partitioned data lake.

Partition layout: {DATA_LAKE_ROOT}/{table_name}/{event_date}/{op_type}/
"""

import os
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/data_lake")
FLUSH_BATCH_SIZE = int(os.environ.get("FLUSH_BATCH_SIZE", "200"))
FLUSH_INTERVAL_SEC = int(os.environ.get("FLUSH_INTERVAL_SEC", "30"))

# partition_key -> list[record]
_buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
_last_flush: Dict[str, float] = defaultdict(float)
_file_counters: Dict[str, int] = defaultdict(int)


def _partition_path(table_name: str, event_date: str, op_type: str) -> str:
    return os.path.join(DATA_LAKE_ROOT, table_name, event_date, op_type)


def _partition_key(table_name: str, event_date: str, op_type: str) -> str:
    return f"{table_name}/{event_date}/{op_type}"


def _infer_arrow_schema(records: List[Dict[str, Any]]) -> pa.Schema:
    """Build a PyArrow schema from the first record's keys, using safe type inference."""
    if not records:
        return pa.schema([])
    sample = records[0]
    fields = []
    for col, val in sample.items():
        if isinstance(val, bool):
            fields.append(pa.field(col, pa.bool_()))
        elif isinstance(val, int):
            fields.append(pa.field(col, pa.int64()))
        elif isinstance(val, float):
            fields.append(pa.field(col, pa.float64()))
        else:
            fields.append(pa.field(col, pa.string()))
    return pa.schema(fields)


def _cast_record(record: Dict[str, Any], schema: pa.Schema) -> Dict[str, Any]:
    """Coerce record values to match the inferred schema types."""
    out = {}
    for field in schema:
        val = record.get(field.name)
        try:
            if field.type == pa.bool_():
                out[field.name] = bool(val) if val is not None else None
            elif field.type == pa.int64():
                out[field.name] = int(val) if val is not None else None
            elif field.type == pa.float64():
                out[field.name] = float(val) if val is not None else None
            else:
                out[field.name] = str(val) if val is not None else None
        except (ValueError, TypeError):
            out[field.name] = str(val) if val is not None else None
    return out


def _flush_partition(key: str, table_name: str, event_date: str, op_type: str) -> None:
    records = _buffers.get(key)
    if not records:
        return

    dir_path = _partition_path(table_name, event_date, op_type)
    os.makedirs(dir_path, exist_ok=True)

    _file_counters[key] += 1
    filename = f"part_{_file_counters[key]:06d}_{int(time.time())}.parquet"
    filepath = os.path.join(dir_path, filename)

    # Collect all column names across all records (handles schema evolution)
    all_keys: List[str] = []
    seen = set()
    for rec in records:
        for k in rec.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    # Build columns dict
    columns: Dict[str, List] = {k: [] for k in all_keys}
    for rec in records:
        for k in all_keys:
            columns[k].append(rec.get(k))

    # Build PyArrow table
    arrays = []
    fields = []
    for col_name, values in columns.items():
        # Infer type from non-None values
        non_null = [v for v in values if v is not None]
        if non_null and isinstance(non_null[0], bool):
            arr = pa.array(values, type=pa.bool_())
            fields.append(pa.field(col_name, pa.bool_()))
        elif non_null and isinstance(non_null[0], int):
            arr = pa.array(values, type=pa.int64())
            fields.append(pa.field(col_name, pa.int64()))
        elif non_null and isinstance(non_null[0], float):
            arr = pa.array(values, type=pa.float64())
            fields.append(pa.field(col_name, pa.float64()))
        else:
            str_values = [str(v) if v is not None else None for v in values]
            arr = pa.array(str_values, type=pa.string())
            fields.append(pa.field(col_name, pa.string()))
        arrays.append(arr)

    schema = pa.schema(fields)
    table = pa.table(dict(zip(all_keys, arrays)), schema=schema)

    pq.write_table(table, filepath, compression="snappy")
    logger.info(
        "Wrote %d records → %s (schema cols: %s)",
        len(records),
        filepath,
        list(all_keys),
    )

    _buffers[key] = []
    _last_flush[key] = time.time()


def buffer_record(
    table_name: str,
    event_date: str,
    op_type: str,
    record: Dict[str, Any],
) -> None:
    """Add a record to the appropriate buffer and flush if thresholds are met."""
    key = _partition_key(table_name, event_date, op_type)
    _buffers[key].append(record)

    should_flush_size = len(_buffers[key]) >= FLUSH_BATCH_SIZE
    should_flush_time = (time.time() - _last_flush[key]) >= FLUSH_INTERVAL_SEC

    if should_flush_size or should_flush_time:
        _flush_partition(key, table_name, event_date, op_type)


def flush_all() -> None:
    """Flush every non-empty buffer. Called on shutdown or timer."""
    for key, records in list(_buffers.items()):
        if records:
            parts = key.split("/")
            table_name, event_date, op_type = parts[0], parts[1], parts[2]
            _flush_partition(key, table_name, event_date, op_type)
