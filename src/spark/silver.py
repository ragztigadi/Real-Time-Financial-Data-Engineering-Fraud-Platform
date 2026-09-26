"""Silver layer: Bronze → parsed, typed, deduplicated."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.settings import get_settings
from src.spark.session import get_spark_session

logger = logging.getLogger(__name__)

# Schema of the envelope payload for aggTrade
AGG_TRADE_SCHEMA = T.StructType([
    T.StructField("e", T.StringType()),
    T.StructField("E", T.LongType()),
    T.StructField("s", T.StringType()),
    T.StructField("a", T.LongType()),
    T.StructField("p", T.StringType()),
    T.StructField("q", T.StringType()),
    T.StructField("f", T.LongType()),
    T.StructField("l", T.LongType()),
    T.StructField("T", T.LongType()),
    T.StructField("m", T.BooleanType()),
])

# Schema of the envelope payload for bookTicker
BOOK_TICKER_SCHEMA = T.StructType([
    T.StructField("u", T.LongType()),
    T.StructField("s", T.StringType()),
    T.StructField("b", T.StringType()),
    T.StructField("B", T.StringType()),
    T.StructField("a", T.StringType()),
    T.StructField("A", T.StringType()),
])

# Top-level envelope schema
ENVELOPE_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("schema_version", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("partition_key", T.StringType()),
    T.StructField("occurred_at", T.StringType()),
    T.StructField("ingested_at", T.StringType()),
    T.StructField("payload", T.StringType()),
])


def process_trades(df):
    """Parse aggTrade envelopes into typed columns."""
    payload = F.from_json(F.col("payload_str"), AGG_TRADE_SCHEMA)

    return (
        df.filter(F.col("event_type") == "aggTrade")
        .withColumn("p", payload)
        .select(
            F.col("event_id"),
            F.col("schema_version"),
            F.col("source"),
            F.col("partition_key").alias("symbol"),
            F.to_timestamp(F.col("occurred_at")).alias("occurred_at"),
            F.to_timestamp(F.col("ingested_at")).alias("ingested_at"),
            F.col("p.a").alias("agg_trade_id"),
            F.col("p.s").alias("raw_symbol"),
            F.col("p.p").cast("decimal(18,8)").alias("price"),
            F.col("p.q").cast("decimal(18,8)").alias("quantity"),
            (F.col("p.p").cast("decimal(18,8)") *
             F.col("p.q").cast("decimal(18,8)")).alias("notional"),
            F.col("p.T").alias("trade_time_ms"),
            F.col("p.m").alias("is_buyer_maker"),
            F.to_timestamp(
                (F.col("p.T") / 1000).cast("long")
            ).alias("trade_time"),
            # partition columns
            F.date_format(F.col("occurred_at"), "yyyy-MM-dd").alias("date"),
            F.date_format(F.col("occurred_at"), "HH").alias("hour"),
        )
        # dedup: same agg_trade_id for same symbol = duplicate
        .dropDuplicates(["symbol", "agg_trade_id"])
    )


def process_quotes(df):
    """Parse bookTicker envelopes into typed columns."""
    payload = F.from_json(F.col("payload_str"), BOOK_TICKER_SCHEMA)

    return (
        df.filter(F.col("event_type") == "bookTicker")
        .withColumn("p", payload)
        .select(
            F.col("event_id"),
            F.col("schema_version"),
            F.col("source"),
            F.col("partition_key").alias("symbol"),
            F.to_timestamp(F.col("occurred_at")).alias("occurred_at"),
            F.to_timestamp(F.col("ingested_at")).alias("ingested_at"),
            F.col("p.u").alias("update_id"),
            F.col("p.b").cast("decimal(18,8)").alias("bid_price"),
            F.col("p.B").cast("decimal(18,8)").alias("bid_qty"),
            F.col("p.a").cast("decimal(18,8)").alias("ask_price"),
            F.col("p.A").cast("decimal(18,8)").alias("ask_qty"),
            (F.col("p.a").cast("decimal(18,8)") -
             F.col("p.b").cast("decimal(18,8)")).alias("spread"),
            # partition columns
            F.date_format(F.col("occurred_at"), "yyyy-MM-dd").alias("date"),
            F.date_format(F.col("occurred_at"), "HH").alias("hour"),
        )
        .dropDuplicates(["symbol", "update_id"])
    )


def run_silver(spark: SparkSession, settings) -> None:
    raw = (
        spark.readStream.format("parquet")
        .option("path", settings.bronze_path)
        .schema(
            T.StructType([
                T.StructField("topic", T.StringType()),
                T.StructField("partition", T.IntegerType()),
                T.StructField("offset", T.LongType()),
                T.StructField("kafka_timestamp", T.TimestampType()),
                T.StructField("partition_key", T.StringType()),
                T.StructField("raw_envelope", T.StringType()),
                T.StructField("date", T.StringType()),
                T.StructField("hour", T.StringType()),
            ])
        )
        .load()
    )

    # parse the envelope wrapper first
    envelope = raw.withColumn(
        "env", F.from_json(F.col("raw_envelope"), ENVELOPE_SCHEMA)
    ).select(
        F.col("env.event_id"),
        F.col("env.event_type"),
        F.col("env.schema_version"),
        F.col("env.source"),
        F.col("env.partition_key"),
        F.col("env.occurred_at"),
        F.col("env.ingested_at"),
        F.col("env.payload").alias("payload_str"),
    )

    trades_silver = process_trades(envelope)
    quotes_silver = process_quotes(envelope)

    trades_query = (
        trades_silver.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{settings.silver_path}/trades")
        .option("checkpointLocation", f"{settings.silver_path}/_checkpoints/trades")
        .partitionBy("date", "hour")
        .trigger(processingTime="60 seconds")
        .start()
    )

    quotes_query = (
        quotes_silver.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{settings.silver_path}/quotes")
        .option("checkpointLocation", f"{settings.silver_path}/_checkpoints/quotes")
        .partitionBy("date", "hour")
        .trigger(processingTime="60 seconds")
        .start()
    )

    logger.info(
        "silver streams started",
        extra={
            "trades_path": f"{settings.silver_path}/trades",
            "quotes_path": f"{settings.silver_path}/quotes",
        },
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    settings = get_settings()
    spark = get_spark_session("silver-transform", settings=settings)
    run_silver(spark, settings)