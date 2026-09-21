from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark_session(app_name: str, *, local: bool = True, settings=None) -> SparkSession:
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    hadoop_aws = "org.apache.hadoop:hadoop-aws:3.3.4"
    aws_sdk = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", f"{kafka_package},{hadoop_aws},{aws_sdk}")
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints/spark")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    if settings and settings.aws_access_key_id:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        )

    if local:
        builder = builder.master("local[*]")

    return builder.getOrCreate()