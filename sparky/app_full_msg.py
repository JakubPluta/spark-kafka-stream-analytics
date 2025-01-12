from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.types import StructType
from core.config import settings
from pyspark.sql import functions as F

from sparky.schema import (
    LoanApplicationSchema,
    CustomerProfileSchema,
    LoanApplicationSchemaWithCustomerProfile,
)
from pyspark.sql.streaming.readwriter import DataStreamReader


def get_spark_session(app_name: str = "SparkStreamingApp") -> SparkSession:
    """
    Returns a SparkSession configured for Spark Streaming.

    Args:
        app_name (str): The name of the application.

    Returns:
        SparkSession: The SparkSession.
    """

    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"
        )
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.streaming.kafka.maxRatePerPartition", "100")
        .config("spark.streaming.backpressure.enabled", "true")
        .getOrCreate()
    )
    return spark


def read_kafka_stream(
    spark: SparkSession, topic: str, schema: StructType
) -> DataFrame | DataStreamReader:
    """
    Reads a Kafka stream and returns a DataFrame with the deserialized values.

    Args:
        spark (SparkSession): The Spark session.
        topic (str): The Kafka topic to read from.
        schema (StructType): The schema of the data.

    Returns:
        pyspark.sql.streaming.DataStreamReader: The DataStreamReader with the deserialized values.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(
            "maxOffsetsPerTrigger", "1"
        )  # Only read one message at a time, as we can't read multiple messages from redis later
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(F.from_json(F.col("value"), schema).alias("data"))
        .select("data.*")
    )


if __name__ == "__main__":
    spark: SparkSession = get_spark_session()

    df: DataFrame = read_kafka_stream(
        spark=spark,
        topic=settings.kafka_input_topic,
        schema=LoanApplicationSchemaWithCustomerProfile,
    )
    df.writeStream.outputMode("append").format("console").start().awaitTermination()
