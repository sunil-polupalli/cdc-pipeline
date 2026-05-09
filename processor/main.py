"""
main.py
CDC Pipeline Processor

Consumes Debezium change events from Kafka, detects schema evolution,
registers schemas, and writes partitioned Parquet files to the data lake.
"""

import json
import logging
import os
import signal
import sys
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import schema_registry
import parquet_writer
from register_connector import ensure_connector_registered
from lineage_report import generate_report

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("processor")

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "cdc-processor-group")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "inventory")
LINEAGE_INTERVAL = int(os.environ.get("LINEAGE_REPORT_INTERVAL_SEC", "60"))

TOPIC_PREFIX = "dbserver1"
TOPICS = [
    f"{TOPIC_PREFIX}.{MYSQL_DATABASE}.customers",
    f"{TOPIC_PREFIX}.{MYSQL_DATABASE}.products",
    f"{TOPIC_PREFIX}.{MYSQL_DATABASE}.orders",
]

_shutdown = threading.Event()


# ── Signal handling ────────────────────────────────────────────────────────────
def _handle_shutdown(signum, frame):
    logger.info("Shutdown signal received, flushing buffers…")
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ── Helpers ────────────────────────────────────────────────────────────────────

def extract_table_name(topic: str) -> str:
    """dbserver1.inventory.products → products"""
    return topic.split(".")[-1]


def extract_schema_from_debezium(payload: Dict) -> Dict[str, str]:
    """
    Extract column-name → type mapping from a Debezium envelope's schema field.
    The 'after' field (or 'before') in the schema describes each column.
    """
    schema_info: Dict[str, str] = {}
    try:
        # Top-level schema block from Debezium JSON message
        fields_list = payload.get("schema", {}).get("fields", [])
        for field in fields_list:
            if field.get("field") in ("after", "before"):
                for col_field in field.get("fields", []):
                    col_name = col_field.get("field", "")
                    col_type = col_field.get("type", "string")
                    if col_name:
                        schema_info[col_name] = col_type
                break
    except Exception as e:
        logger.warning("Could not extract schema from payload: %s", e)
    return schema_info


def parse_debezium_message(raw_value: bytes) -> Optional[Dict[str, Any]]:
    """Parse a raw Debezium Kafka message value into a structured dict."""
    if raw_value is None:
        return None
    try:
        msg = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Failed to decode message: %s", e)
        return None

    payload = msg.get("payload", {})
    if not payload:
        return None

    op = payload.get("op")  # c, u, d, r (read/snapshot)
    if op not in ("c", "u", "d", "r"):
        return None
    # Treat snapshot reads same as creates
    if op == "r":
        op = "c"

    # Get data – after for inserts/updates, before for deletes
    data = payload.get("after") if op in ("c", "u") else payload.get("before")
    if data is None:
        return None

    # Event timestamp from Debezium ts_ms
    ts_ms = payload.get("ts_ms", int(time.time() * 1000))
    event_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    event_date = event_dt.strftime("%Y-%m-%d")
    event_timestamp = event_dt.isoformat()

    # Extract schema definition from message envelope
    schema_def = extract_schema_from_debezium(msg)

    return {
        "op": op,
        "data": data,
        "event_date": event_date,
        "event_timestamp": event_timestamp,
        "schema_def": schema_def,
    }


def process_message(topic: str, raw_value: bytes) -> None:
    table_name = extract_table_name(topic)
    parsed = parse_debezium_message(raw_value)
    if parsed is None:
        return

    op = parsed["op"]
    data = parsed["data"]
    event_date = parsed["event_date"]
    event_timestamp = parsed["event_timestamp"]
    schema_def = parsed["schema_def"]

    # Schema registration (detect evolution)
    if schema_def:
        schema_version, is_new = schema_registry.get_or_register_schema(
            table_name, schema_def
        )
        if is_new:
            logger.info(
                "🔄 Schema evolution detected for '%s' → version %d",
                table_name,
                schema_version,
            )
    else:
        # Fallback: derive schema from data keys
        fallback_schema = {k: "string" for k in data.keys()}
        schema_version, _ = schema_registry.get_or_register_schema(
            table_name, fallback_schema
        )

    # Enrich record with pipeline metadata
    enriched = dict(data)
    enriched["_op_type"] = op
    enriched["_event_timestamp"] = event_timestamp
    enriched["_schema_version"] = schema_version

    # Coerce numeric-looking values
    for k, v in enriched.items():
        if isinstance(v, dict):
            # Debezium wraps some types (e.g. Decimal) as {"scale":2,"value":"..."}
            enriched[k] = str(v)

    parquet_writer.buffer_record(table_name, event_date, op, enriched)


def wait_for_kafka(timeout: int = 300) -> KafkaConsumer:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=None,
                consumer_timeout_ms=1000,
                max_poll_records=500,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info("Connected to Kafka bootstrap=%s", KAFKA_BOOTSTRAP)
            return consumer
        except NoBrokersAvailable:
            logger.info("Waiting for Kafka to be available…")
            time.sleep(5)
    raise RuntimeError("Kafka did not become available in time.")


def lineage_reporter_loop() -> None:
    """Background thread: regenerate the lineage report periodically."""
    while not _shutdown.is_set():
        try:
            generate_report()
        except Exception as e:
            logger.warning("Lineage report generation failed: %s", e)
        _shutdown.wait(timeout=LINEAGE_INTERVAL)
    # Final report on shutdown
    try:
        generate_report()
    except Exception:
        pass


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=== CDC Pipeline Processor starting ===")

    # Init schema registry DB
    schema_registry.init_db()

    # Register Debezium connector (waits for Connect to be ready)
    logger.info("Registering Debezium connector…")
    try:
        ensure_connector_registered()
    except RuntimeError as e:
        logger.error("Connector registration failed: %s", e)
        sys.exit(1)

    # Start lineage reporter background thread
    reporter = threading.Thread(target=lineage_reporter_loop, daemon=True)
    reporter.start()

    # Connect to Kafka
    consumer = wait_for_kafka()

    processed = 0
    last_flush_time = time.time()
    PERIODIC_FLUSH_SEC = int(os.environ.get("FLUSH_INTERVAL_SEC", "30"))

    logger.info("Starting to consume from topics: %s", TOPICS)

    try:
        while not _shutdown.is_set():
            records = consumer.poll(timeout_ms=2000, max_records=500)

            if records:
                for topic_partition, messages in records.items():
                    topic = topic_partition.topic
                    for msg in messages:
                        try:
                            process_message(topic, msg.value)
                            processed += 1
                        except Exception as e:
                            logger.error(
                                "Error processing message on %s offset %d: %s",
                                topic,
                                msg.offset,
                                e,
                                exc_info=True,
                            )

                # Commit offsets only after successful processing (at-least-once)
                consumer.commit()

                if processed % 1000 == 0 and processed > 0:
                    logger.info("Processed %d messages total", processed)

            # Periodic flush regardless of batch size
            now = time.time()
            if now - last_flush_time >= PERIODIC_FLUSH_SEC:
                parquet_writer.flush_all()
                last_flush_time = now

    finally:
        logger.info("Flushing all remaining buffers…")
        parquet_writer.flush_all()
        consumer.close()
        logger.info("Processor shut down. Total messages processed: %d", processed)


if __name__ == "__main__":
    main()
