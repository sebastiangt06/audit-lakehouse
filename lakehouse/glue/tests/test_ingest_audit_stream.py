"""Unit tests for the pure PySpark functions of the Glue jobs (no Kafka, no catalog).

Run inside the Glue image (spark-submit puts the image's own pyspark 3.5.4-amzn first on the path;
plain `python3 -m pytest` would pick the pip pyspark 3.5.2 instead):

    docker compose run --rm glue-jobs spark-submit tests/test_ingest_audit_stream.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs"))

from create_tables import BRONZE_PROPERTIES, bronze_ddl  # noqa: E402
from ingest_audit_stream import parse_events  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

KAFKA_SCHEMA = "value binary, partition int, offset long"


def payload_json(amount: str) -> str:
    """Payload exactly as the .NET outbox writes it (camelCase, compact, money as a string)."""
    return (
        '{"eventId":"019ac0a1-0000-7000-8000-000000000001","occurredAt":"2026-11-26T03:51:58.7017221Z",'
        '"transactionId":"019ac0a1-0000-7000-8000-000000000002","tenantId":"t1","actorId":"loadgen",'
        f'"from":"A1","to":"B2","amount":{amount},"currency":"COP",'
        '"eventType":"transaction.registered","schemaVersion":1}'
    )


def envelope(payload: str) -> bytes:
    """Kafka value as KafkaEventBusPublisher serializes it (payload embedded as a raw object)."""
    return (
        '{"event_id":"019ac0a1-0000-7000-8000-000000000001","event_type":"transaction.registered",'
        '"aggregate_type":"Account","aggregate_id":"t1:A1","tenant_id":"t1","actor_id":"loadgen",'
        '"occurred_at":"2026-11-26T03:51:58.701722Z","schema_version":1,'
        f'"payload":{payload},"trace_id":"1ef8b840107039e884845277295d848b"}}'
    ).encode("utf-8")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("glue-unit-tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.mark.parametrize("amount", ["12345678.1234", "123456789012345.6789"])
def test_money_as_string_reaches_bronze_payload_identical(spark, amount):
    raw_payload = payload_json(json.dumps(amount))  # "amount":"12345678.1234"
    kafka = spark.createDataFrame([(envelope(raw_payload), 3, 41)], KAFKA_SCHEMA)

    row = parse_events(kafka).first()

    assert row["payload"] == raw_payload  # byte-identical text, not just equivalent JSON
    assert json.loads(row["payload"])["amount"] == amount


def test_money_as_json_number_would_be_corrupted(spark):
    """Why the contract carries money as strings: Jackson rewrites JSON numbers through double."""
    raw_payload = payload_json("12345678.1234")  # "amount":12345678.1234 (a JSON number)
    kafka = spark.createDataFrame([(envelope(raw_payload), 0, 0)], KAFKA_SCHEMA)

    stored = parse_events(kafka).first()["payload"]

    assert '"amount":1.23456781234E7' in stored
    assert stored != raw_payload


def test_envelope_fields_lineage_and_types(spark):
    kafka = spark.createDataFrame([(envelope(payload_json('"150000"')), 5, 280)], KAFKA_SCHEMA)

    df = parse_events(kafka)
    row = df.first()

    assert dict(df.dtypes)["occurred_at"] == "timestamp"
    assert row["occurred_at"] == datetime(2026, 11, 26, 3, 51, 58, 701722)  # UTC session, microseconds
    assert (row["event_id"], row["tenant_id"], row["aggregate_id"]) == (
        "019ac0a1-0000-7000-8000-000000000001", "t1", "t1:A1")
    assert row["schema_version"] == 1
    assert (row["kafka_partition"], row["kafka_offset"]) == (5, 280)
    assert row["ingested_at"] is not None


def test_malformed_message_is_kept_not_dropped(spark):
    kafka = spark.createDataFrame([(b"not json", 2, 7)], KAFKA_SCHEMA)

    rows = parse_events(kafka).collect()

    assert len(rows) == 1
    assert rows[0]["event_id"] is None
    assert (rows[0]["kafka_partition"], rows[0]["kafka_offset"]) == (2, 7)


def test_bronze_ddl_follows_adr_006():
    ddl = "\n".join(bronze_ddl())

    assert "PARTITIONED BY (days(occurred_at))" in ddl
    assert "WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY tenant_id, occurred_at" in ddl
    assert "CREATE TABLE IF NOT EXISTS lake.audit.events_bronze" in ddl
    for key, value in BRONZE_PROPERTIES.items():
        assert f"'{key}' = '{value}'" in ddl
    assert BRONZE_PROPERTIES["write.distribution-mode"] == "hash"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-p", "no:cacheprovider"]))
