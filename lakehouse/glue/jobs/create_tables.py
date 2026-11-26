"""AWS Glue job: create the Iceberg bronze table for audit events (idempotent).

Runs unchanged in AWS Glue 5.0 (GlueContext / getResolvedOptions / Job) and locally in the
official aws-glue-libs:5 image:

    docker compose run --rm glue-jobs spark-submit jobs/create_tables.py --JOB_NAME create_tables

Table design (ADR-006):
- PARTITIONED BY days(occurred_at): hidden partitioning, queries filter on occurred_at directly.
  Never hours() or the raw timestamp (too many partitions, tiny files).
- WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY tenant_id, occurred_at: hash distribution by
  partition plus a sort order. Plain `WRITE ORDERED BY` would switch write.distribution-mode to
  `range`, contradicting the `hash` we want.
- format v2, zstd, 128 MB target files, metadata cleanup after commit (keep 50 versions).
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

BRONZE_TABLE = "lake.audit.events_bronze"

BRONZE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": "134217728",
    "write.distribution-mode": "hash",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}


def bronze_ddl(table: str = BRONZE_TABLE) -> list[str]:
    """Statements that create the namespace and the bronze table. Pure: no Spark needed."""
    namespace = table.rsplit(".", 1)[0]
    properties = ",\n  ".join(f"'{k}' = '{v}'" for k, v in BRONZE_PROPERTIES.items())
    return [
        f"CREATE NAMESPACE IF NOT EXISTS {namespace}",
        f"""CREATE TABLE IF NOT EXISTS {table} (
  event_id        string    COMMENT 'UUIDv7 from the core; end-to-end dedupe key (silver)',
  event_type      string,
  aggregate_type  string,
  aggregate_id    string    COMMENT 'Kafka message key: per-aggregate ordering',
  tenant_id       string,
  actor_id        string,
  occurred_at     timestamp COMMENT 'Business time (UTC); partition source',
  schema_version  int,
  payload         string    COMMENT 'Event payload, raw JSON; money as exact strings',
  trace_id        string,
  kafka_partition int       COMMENT 'Lineage: source partition',
  kafka_offset    bigint    COMMENT 'Lineage: source offset (replay / reconciliation)',
  ingested_at     timestamp COMMENT 'Ingestion time (UTC)'
)
USING iceberg
PARTITIONED BY (days(occurred_at))
TBLPROPERTIES (
  {properties}
)""",
        f"ALTER TABLE {table} WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY tenant_id, occurred_at",
    ]


def ensure_bronze(spark: SparkSession, table: str = BRONZE_TABLE) -> bool:
    """Create the bronze table if missing. Returns True if it was created.

    Idempotent without side effects: an existing table is left untouched (no new metadata
    version), so running the job again changes nothing.
    """
    if spark.catalog.tableExists(table):
        print(f"[create_tables] {table} already exists; nothing to do")
        return False

    for statement in bronze_ddl(table):
        spark.sql(statement)
    print(f"[create_tables] created {table}")
    return True


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    ensure_bronze(spark)
    spark.sql(f"SHOW TBLPROPERTIES {BRONZE_TABLE}").show(truncate=False)

    job.commit()


if __name__ == "__main__":
    main()
