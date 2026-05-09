"""
schema_registry.py
SQLite-backed schema store that versions table schemas and detects evolution.
"""

import sqlite3
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("SCHEMA_DB_PATH", "/state/schemas.db")


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the table_schemas table if it doesn't exist."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS table_schemas (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name      TEXT    NOT NULL,
                schema_version  INTEGER NOT NULL,
                schema_definition TEXT  NOT NULL,
                created_at      TEXT    NOT NULL,
                active_to       TEXT,
                UNIQUE(table_name, schema_version)
            )
        """)
        conn.commit()
    logger.info("Schema registry initialised at %s", DB_PATH)


def _canonical(schema: Dict[str, str]) -> str:
    """Produce a stable JSON string for schema comparison."""
    return json.dumps(schema, sort_keys=True)


def get_or_register_schema(
    table_name: str, schema: Dict[str, str]
) -> Tuple[int, bool]:
    """
    Look up schema for table_name. If new, register it and return (version, True).
    If already known, return (version, False).

    schema is a dict of {column_name: type_string}.
    Returns (schema_version, is_new).
    """
    canonical = _canonical(schema)
    now = datetime.now(timezone.utc).isoformat()

    with _get_conn() as conn:
        # Check if this exact schema is already registered
        row = conn.execute(
            "SELECT schema_version FROM table_schemas WHERE table_name=? AND schema_definition=?",
            (table_name, canonical),
        ).fetchone()

        if row:
            return row["schema_version"], False

        # New schema – find latest version for this table
        latest = conn.execute(
            "SELECT MAX(schema_version) AS mv FROM table_schemas WHERE table_name=?",
            (table_name,),
        ).fetchone()
        new_version = (latest["mv"] or 0) + 1

        # Mark previous schema version as inactive
        if new_version > 1:
            conn.execute(
                "UPDATE table_schemas SET active_to=? WHERE table_name=? AND active_to IS NULL",
                (now, table_name),
            )

        conn.execute(
            """
            INSERT INTO table_schemas (table_name, schema_version, schema_definition, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (table_name, new_version, canonical, now),
        )
        conn.commit()
        logger.info(
            "Registered schema v%d for table '%s'", new_version, table_name
        )
        return new_version, True


def get_all_schemas() -> list:
    """Return all schema records (for lineage report generation)."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM table_schemas ORDER BY table_name, schema_version"
        ).fetchall()
    return [dict(r) for r in rows]


def get_schema_by_version(table_name: str, version: int) -> Optional[Dict]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM table_schemas WHERE table_name=? AND schema_version=?",
            (table_name, version),
        ).fetchone()
    return dict(row) if row else None
