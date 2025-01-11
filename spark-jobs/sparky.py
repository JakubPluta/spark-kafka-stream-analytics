from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)
from pyspark.sql import DataFrame, DataFrameWriter, DataFrameReader
from config import Settings
from schemas import (
    VEHICLE_SCHEMA,
    GPS_SCHEMA,
    TRAFFIC_SCHEMA,
    WEATHER_SCHEMA,
    EMERGENCY_SCHEMA,
)


def read_kafka_stream_from_topic(spark: SparkSession, topic, schema):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "60 seconds")
    )


def write_kafka_stream_to_parquet(
        input_df: DataFrame | DataFrameReader, checkpoint_location, output_path
):
    return (
        input_df.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_location)
        .option("path", output_path)
        .outputMode("append")
        .start()
    )


def write_kafka_stream_to_console(input_df: DataFrame | DataFrameReader):
    return input_df.writeStream.format("console").outputMode("append").start()


def main():
    spark = (
        SparkSession.builder.appName("Sparky")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk:1.12.779",
        )
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", Settings.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", Settings.S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", Settings.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    # spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setLogLevel("WARN")

    vehicle_df = read_kafka_stream_from_topic(
        spark, Settings.VEHICLE_TOPIC, VEHICLE_SCHEMA
    )
    gps_df = read_kafka_stream_from_topic(spark, Settings.GPS_TOPIC, GPS_SCHEMA)
    traffic_df = read_kafka_stream_from_topic(
        spark, Settings.TRAFFIC_TOPIC, TRAFFIC_SCHEMA
    )
    weather_df = read_kafka_stream_from_topic(
        spark, Settings.WEATHER_TOPIC, WEATHER_SCHEMA
    )
    emergency_df = read_kafka_stream_from_topic(
        spark, Settings.EMERGENCY_TOPIC, EMERGENCY_SCHEMA
    )

    q1 = write_kafka_stream_to_parquet(
        vehicle_df,
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_CHECKPOINT_PREFIX}/vehicle",
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_OUTPUT_PREFIX}/vehicle",
    )

    q2 = write_kafka_stream_to_parquet(
        gps_df,
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_CHECKPOINT_PREFIX}/gps",
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_OUTPUT_PREFIX}/gps",
    )

    q3 = write_kafka_stream_to_parquet(
        traffic_df,
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_CHECKPOINT_PREFIX}/traffic",
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_OUTPUT_PREFIX}/traffic",
    )

    q4 = write_kafka_stream_to_parquet(
        weather_df,
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_CHECKPOINT_PREFIX}/weather",
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_OUTPUT_PREFIX}/weather",
    )

    q5 = write_kafka_stream_to_parquet(
        emergency_df,
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_CHECKPOINT_PREFIX}/emergency",
        f"s3a://{Settings.S3_BUCKET}/{Settings.S3_OUTPUT_PREFIX}/emergency",
    )

    q5.awaitTermination()


if __name__ == "__main__":
    main()
