from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.settings import get_settings
from src.spark.session import get_spark_session


logger = logging.getLogger(__name__)

SILVER_TRADES_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("schema_version", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("symbol", T.StringType()),
    T.StructField("occurred_at", T.TimestampType()),
    T.StructField("ingested_at", T.TimestampType()),
    T.StructField("agg_trade_id", T.LongType()),
    T.StructField("raw_symbol", T.StringType()),
    T.StructField("price", T.DecimalType(18, 8)),
    T.StructField("quantity", T.DecimalType(18, 8)),
    T.StructField("notional", T.DecimalType(37, 16)),
    T.StructField("trade_time_ms", T.LongType()),
    T.StructField("is_buyer_maker", T.BooleanType()),
    T.StructField("trade_time", T.TimestampType()),
    T.StructField("date", T.DateType()),
    T.StructField("hour", T.IntegerType()),
])

def run_gold(spark: SparkSession, settings)->None:
    trades = (
        spark.readStream.format("parquet")
        .option("path", f"{settings.silver_path}/trades")
        .schema(SILVER_TRADES_SCHEMA)
        .load()
    )

    # 1. Per-symbol volume summary — 1 minute windows
    # Used by: velocity checks, amount anomaly detection

    volume_agg = (
        trades
        .withWatermark("occurred_at", "5 minutes")
        .groupBy(
            F.window(F.col("occurred_at"), "1 minute"),
            F.col("symbol"),
        )
        .agg(
            F.count("*").alias("trade_count"),
            F.sum(F.col("notional")).alias("total_notional"),
            F.avg(F.col("notional")).alias("avg_notional"),
            F.max(F.col("notional")).alias("max_notional"),
            F.avg(F.col("price")).alias("vwap"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("trade_count"),
            F.col("total_notional"),
            F.col("avg_notional"),
            F.col("max_notional"),
            F.col("vwap"),
            F.date_format(F.col("window.start"), "yyyy-MM-dd").alias("date"),
            F.date_format(F.col("window.start"), "HH").alias("hour"),
        )
    )

    # 2. Buyer/seller imbalance — feeds pattern detection

    side_agg = (
        trades
        .withWatermark("occurred_at", "5 minutes")
        .groupBy(
            F.window("occurred_at", "1 minute"),
            F.col("symbol"),
            F.col("is_buyer_maker"),
        )
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("notional").alias("total_notional"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("symbol"),
            F.col("is_buyer_maker"),
            F.col("trade_count"),
            F.col("total_notional"),
            F.date_format(F.col("window.start"), "yyyy-MM-dd").alias("date"),
            F.date_format(F.col("window.start"), "HH").alias("hour"),
        )
    )

    volume_query = (
        volume_agg.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{settings.gold_path}/volume_1m")
        .option("checkpointLocation",
                f"{settings.gold_path}/_checkpoints/volume_1m")
        .partitionBy("date","hour")
        .trigger(processingTime="60 seconds")
        .start()
    )

    side_query = (
        side_agg.writeStream
        .format("parquet")
        .option("path", f"{settings.gold_path}/side_imbalance_1m")
        .option("checkpointLocation",
                f"{settings.gold_path}/_checkpoints/side_imbalance_1m")
        .partitionBy("date", "hour")
        .trigger(processingTime="60 seconds")
        .start()
    )

    logger.info(
        "gold streams started",
        extra={
            "volume_path": f"{settings.gold_path}/volume_1m",
            "side_path": f"{settings.gold_path}/side_imbalance_1m",
        },
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    settings = get_settings()
    spark = get_spark_session("gold-aggregate", settings=settings)
    run_gold(spark, settings)
