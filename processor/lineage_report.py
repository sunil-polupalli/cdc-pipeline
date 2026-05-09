"""
lineage_report.py
Generates output/lineage_report.json by combining schema registry data
with the actual partition paths found in the data lake.
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from schema_registry import get_all_schemas

logger = logging.getLogger(__name__)

DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/data_lake")
REPORT_PATH = os.environ.get("LINEAGE_REPORT_PATH", "/output/lineage_report.json")


def _discover_partitions(table_name: str) -> List[str]:
    """Walk the data lake and collect partition paths that belong to table_name."""
    table_root = os.path.join(DATA_LAKE_ROOT, table_name)
    partitions = []
    if not os.path.isdir(table_root):
        return partitions
    for event_date in sorted(os.listdir(table_root)):
        date_path = os.path.join(table_root, event_date)
        if not os.path.isdir(date_path):
            continue
        for op_type in sorted(os.listdir(date_path)):
            op_path = os.path.join(date_path, op_type)
            if os.path.isdir(op_path) and any(
                f.endswith(".parquet") for f in os.listdir(op_path)
            ):
                partitions.append(f"/data_lake/{table_name}/{event_date}/{op_type}/")
    return partitions


def _parse_schema_definition(schema_def_json: str) -> Dict[str, str]:
    try:
        return json.loads(schema_def_json)
    except (json.JSONDecodeError, TypeError):
        return {}


def _debezium_type_to_simple(dtype: str) -> str:
    dtype_lower = str(dtype).lower()
    if "int" in dtype_lower:
        return "int"
    if "varchar" in dtype_lower or "text" in dtype_lower or "char" in dtype_lower:
        return "string"
    if "decimal" in dtype_lower or "float" in dtype_lower or "double" in dtype_lower:
        return "decimal"
    if "timestamp" in dtype_lower or "datetime" in dtype_lower or "date" in dtype_lower:
        return "timestamp"
    if "bool" in dtype_lower or "tinyint(1)" in dtype_lower:
        return "boolean"
    return dtype_lower


def generate_report() -> None:
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    all_schemas = get_all_schemas()
    if not all_schemas:
        logger.warning("No schemas found in registry – writing empty report.")
        with open(REPORT_PATH, "w") as f:
            json.dump([], f, indent=2)
        return

    entries: List[Dict[str, Any]] = []

    for rec in all_schemas:
        table_name = rec["table_name"]
        version = rec["schema_version"]
        created_at = rec["created_at"]
        active_to = rec.get("active_to")

        schema_def = _parse_schema_definition(rec["schema_definition"])
        # Convert raw types to simple names
        output_schema = {
            col: _debezium_type_to_simple(typ) for col, typ in schema_def.items()
        }

        partitions = _discover_partitions(table_name)

        entry = {
            "source_table": f"inventory.{table_name}",
            "schema_version": version,
            "active_from": created_at,
            "active_to": active_to,
            "transformation_logic": "Debezium CDC event processing, direct mapping",
            "output_partitions": partitions,
            "output_schema": output_schema,
        }
        entries.append(entry)

    with open(REPORT_PATH, "w") as f:
        json.dump(entries, f, indent=2, default=str)

    logger.info("Lineage report written to %s (%d entries)", REPORT_PATH, len(entries))
    print(f"[lineage] Report written → {REPORT_PATH} ({len(entries)} entries)")
