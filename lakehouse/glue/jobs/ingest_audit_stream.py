"""AWS Glue streaming job: Kafka `audit.events` -> Iceberg `lake.audit.events_bronze` (append-only).

Runs unchanged in AWS Glue 5.0 and locally in aws-glue-libs:5 (service `glue-ingest`):

    spark-submit jobs/ingest_audit_stream.py --JOB_NAME ingest_audit_stream

Settings come from environment variables (defaults in parentheses):
    KAFKA_BOOTSTRAP (kafka:9092), AUDIT_TOPIC (audit.events),
    INGEST_TRIGGER_SECONDS (60), MAX_OFFSETS_PER_TRIGGER (50000),
    CHECKPOINT_DIR (/checkpoints/audit_bronze)

Guarantees:
- Exactly-once into bronze: the Iceberg streaming sink commits one snapshot per micro-batch and
  records the query id + epoch in it; together with the checkpoint (Kafka offsets), a batch that
  is retried after a crash is not appended twice. Upstream delivery is at-least-once (outbox
  relay), so duplicates by event_id can still exist and are removed in silver (ADR-005).
- No silent loss: failOnDataLoss=true stops the query if offsets were deleted by retention before
  being read, instead of skipping them. Malformed messages are NOT dropped: they land with null
  envelope fields and their kafka_partition / kafka_offset, so they stay traceable.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

# Sibling module: on spark-submit the script's directory is on sys.path. In AWS Glue, ship it with
# --extra-py-files s3://.../create_tables.py.
from create_tables import BRONZE_TABLE, ensure_bronze

# Wire contract written by the .NET relay (KafkaEventBusPublisher, ADR-001). `payload` is a nested
# JSON object; declaring it as StringType makes from_json return its JSON text. That text is
# faithful because money travels as JSON strings ("amount":"12345678.1234"): Jackson re-serializes
# JSON numbers through double, which is why amounts must never be JSON numbers.
ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("aggregate_type", T.StringType()),
        T.StructField("aggregate_id", T.StringType()),
        T.StructField("tenant_id", T.StringType()),
        T.StructField("actor_id", T.StringType()),
        T.StructField("occurred_at", T.StringType()),
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("payload", T.StringType()),
        T.StructField("trace_id", T.StringType()),
    ]
)

# "2026-11-26T03:51:58.701722Z": microseconds, the precision of Spark/Iceberg timestamps.
OCCURRED_AT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    topic: str
    # 60 s by default: each trigger commits at least one data file per touched day partition plus
    # a snapshot and manifests. Seconds-level triggers would multiply small files and metadata
    # (slower planning, more S3 requests) to win latency that audit does not need; 60 s keeps bronze
    # about a minute behind the core, and compaction (step 07) merges what remains. ADR-006.
    trigger_seconds: int
    # Caps a batch after downtime (catch-up from a backlog) so memory stays bounded.
    max_offsets_per_trigger: int
    checkpoint_dir: str

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            topic=os.environ.get("AUDIT_TOPIC", "audit.events"),
            trigger_seconds=int(os.environ.get("INGEST_TRIGGER_SECONDS", "60")),
            max_offsets_per_trigger=int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000")),
            checkpoint_dir=os.environ.get("CHECKPOINT_DIR", "/checkpoints/audit_bronze"),
        )


def parse_events(kafka_df: DataFrame) -> DataFrame:
    """Kafka records (value, partition, offset) -> bronze rows. Pure transformation, batch or stream."""
    envelope = F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA)
    return kafka_df.select(
        envelope.alias("e"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.aggregate_type").alias("aggregate_type"),
        F.col("e.aggregate_id").alias("aggregate_id"),
        F.col("e.tenant_id").alias("tenant_id"),
        F.col("e.actor_id").alias("actor_id"),
        F.to_timestamp(F.col("e.occurred_at"), OCCURRED_AT_FORMAT).alias("occurred_at"),
        F.col("e.schema_version").alias("schema_version"),
        F.col("e.payload").alias("payload"),
        F.col("e.trace_id").alias("trace_id"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset").cast("bigint").alias("kafka_offset"),
        F.current_timestamp().alias("ingested_at"),
    )


def start_ingest(spark: SparkSession, settings: Settings) -> StreamingQuery:
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        # Only used on the very first run; afterwards the checkpoint decides where to resume.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )
    return (
        parse_events(source)
        .writeStream.format("iceberg")
        .outputMode("append")
        .queryName("audit_bronze")
        .trigger(processingTime=f"{settings.trigger_seconds} seconds")
        .option("checkpointLocation", settings.checkpoint_dir)
        # The streaming sink does not cluster rows by partition before writing; fanout keeps one
        # open file per partition instead of failing on unclustered input. The sort order
        # (tenant_id, occurred_at) is applied by compaction (step 07).
        .option("fanout-enabled", "true")
        .toTable(BRONZE_TABLE)
    )


def _log_progress(spark: SparkSession) -> None:
    from pyspark.sql.streaming import StreamingQueryListener

    class ProgressLogger(StreamingQueryListener):
        def onQueryStarted(self, event):  # noqa: N802 (Spark API)
            print(f"[ingest] query started id={event.id}", flush=True)

        def onQueryProgress(self, event):  # noqa: N802
            p = event.progress
            print(f"[ingest] batch={p.batchId} rows={p.numInputRows} durationMs={p.batchDuration}", flush=True)

        def onQueryIdle(self, event):  # noqa: N802
            pass

        def onQueryTerminated(self, event):  # noqa: N802
            print(f"[ingest] query terminated id={event.id} exception={event.exception}", flush=True)

    spark.streams.addListener(ProgressLogger())


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

    settings = Settings.from_env()
    print(f"[ingest] {settings}", flush=True)
    ensure_bronze(spark)  # idempotent: `make up` needs no separate init service
    _log_progress(spark)

    query = start_ingest(spark, settings)
    query.awaitTermination()

    # Not reached while the stream runs: awaitTermination() blocks until the query stops or fails
    # (a failure raises and the container restarts). That is fine because this job's progress is
    # not tracked by Glue job bookmarks (what job.commit() persists) but by the streaming
    # checkpoint (Kafka offsets) and the Iceberg snapshots, both committed every micro-batch.
    # Kept so a graceful stop ends the job the Glue way.
    job.commit()


if __name__ == "__main__":
    main()
