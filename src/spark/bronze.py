"""Bronze layer: Kafka → S3 raw landing zone. """

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import get_settings
from src.spark.session import get_spark_session

logger = logging.getLogger(__name__)


def run_bronze(spark: SparkSession, settings) -> None:
    kafka_options = {
        "kafka.bootstrap.servers": settings.kafka_bootstrap_servers,
        "subscribe": f"{settings.kafka_topic_trades},{settings.kafka_topic_quotes}",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "50000",
    }

    raw = (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
    )
    
    bronze = raw.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("partition_key"),
        F.col("value").cast("string").alias("raw_envelope"),
        F.date_format(F.col("timestamp"), "yyyy-MM-dd").alias("date"),
        F.date_format(F.col("timestamp"), "HH").alias("hour"),
    )

    checkpoint = f"{settings.bronze_path}/_checkpoints"

    query = (
        bronze.writeStream.format("parquet")
        .outputMode("append")
        .option("path", settings.bronze_path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("date", "hour", "topic")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("bronze stream started", extra={"path": settings.bronze_path})
    query.awaitTermination()


if __name__ == "__main__":
    settings = get_settings()
    spark = get_spark_session("bronze-ingest")
    run_bronze(spark, settings)