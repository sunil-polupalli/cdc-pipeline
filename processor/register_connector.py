"""
register_connector.py
Registers the Debezium MySQL connector via the Kafka Connect REST API.
Retries until Connect is ready.
"""

import json
import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://connect:8083")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_USER = os.environ.get("DEBEZIUM_USER", "debezium_user")
MYSQL_PASSWORD = os.environ.get("DEBEZIUM_PASSWORD", "debezium_pw")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "inventory")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

CONNECTOR_NAME = "mysql-cdc-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": MYSQL_HOST,
        "database.port": MYSQL_PORT,
        "database.user": MYSQL_USER,
        "database.password": MYSQL_PASSWORD,
        "database.server.id": "184054",
        "topic.prefix": "dbserver1",
        "database.include.list": MYSQL_DATABASE,
        "table.include.list": f"{MYSQL_DATABASE}.customers,{MYSQL_DATABASE}.orders,{MYSQL_DATABASE}.products",
        "schema.history.internal.kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "schema.history.internal.kafka.topic": "schema-changes.inventory",
        "include.schema.changes": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enabled": "true",
        "value.converter.schemas.enabled": "true",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",
        "binary.handling.mode": "bytes",
        "time.precision.mode": "connect",
    },
}


def wait_for_connect(timeout: int = 300) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if resp.status_code == 200:
                logger.info("Kafka Connect is ready.")
                return True
        except requests.exceptions.RequestException:
            pass
        logger.info("Waiting for Kafka Connect…")
        time.sleep(5)
    return False


def connector_exists() -> bool:
    try:
        resp = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}", timeout=5)
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False


def register_connector() -> bool:
    if connector_exists():
        logger.info("Connector '%s' already registered, skipping.", CONNECTOR_NAME)
        return True

    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(CONNECTOR_CONFIG),
        timeout=30,
    )
    if resp.status_code in (200, 201):
        logger.info("Connector registered successfully.")
        return True
    else:
        logger.error(
            "Failed to register connector: %d %s", resp.status_code, resp.text
        )
        return False


def ensure_connector_registered(max_retries: int = 10) -> None:
    if not wait_for_connect():
        raise RuntimeError("Kafka Connect did not become ready in time.")
    for attempt in range(max_retries):
        if register_connector():
            return
        logger.warning("Attempt %d/%d failed, retrying in 10s…", attempt + 1, max_retries)
        time.sleep(10)
    raise RuntimeError("Could not register Debezium connector after multiple attempts.")
