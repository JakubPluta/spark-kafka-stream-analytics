from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.types import StructType
from core.config import settings
from pyspark.sql import functions as F

from sparky.schema import LoanApplicationSchema, CustomerProfileSchema
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
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
            "com.redislabs:spark-redis_2.12:3.1.0",
        )
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.streaming.kafka.maxRatePerPartition", "100")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.redis.host", "localhost")
        .config("spark.redis.port", "6379")
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


def write_to_kafka(df: DataFrame, topic: str):
    """
    Writes a DataFrame to a Kafka topic.

    Args:
        df (DataFrame): The DataFrame to write.
        topic (str): The Kafka topic to write to.


    Returns:

    """
    return (
        df.write.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_brokers)
        .option("topic", topic)
        .option("checkpointLocation", "./checkpoint")
    ).save()


def read_from_redis(spark: SparkSession) -> DataFrame:
    """
    Reads data from Redis and returns a DataFrame.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        DataFrame: The DataFrame containing the data from Redis.
    """
    # Read data from Redis using the Spark Redis connector
    return (
        spark.read.format("org.apache.spark.sql.redis")
        .option("table", "customer")
        .option("infer.schema", "true")
        .load()
        .select(F.from_json(F.col("data"), CustomerProfileSchema).alias("data"))
        .select("data.*")
    )


def _read_single_customer_from_redis(
    spark: SparkSession, customer_id: int
) -> DataFrame:
    # Read data from Redis using the Spark Redis connector
    return (
        spark.read.format("org.apache.spark.sql.redis")
        .option("keys.pattern", f"customer:{customer_id}")
        .option("infer.schema", "true")
        .load()
        .select(F.from_json(F.col("data"), CustomerProfileSchema).alias("data"))
        .select("data.*")
    )


def analyze_risk(df: DataFrame) -> DataFrame:
    """
    Analyze the risk of the customers based on their applications and profiles.

    Args:
        df (DataFrame): The DataFrame containing the customer applications and profiles.

    Returns:
        DataFrame: The DataFrame containing the aggregated risk data.
    """
    return (
        df.groupby(
            "customer_id",
            "customer_segment",
            "financial_profile.employment_type",
            "loan_purpose",
        )
        .agg(
            F.count("*").alias("applications_count"),
            F.avg("risk_assessment_score").alias("avg_risk_score"),
            F.avg("financial_profile.credit_score").alias("avg_credit_score"),
            F.avg("requested_amount").alias("avg_requested_amount"),
            F.sum(
                F.when(F.col("automated_decision") == "APPROVED", 1).otherwise(0)
            ).alias("approved_count"),
            F.avg("behavioral_metrics.investment_risk_score").alias(
                "avg_investment_risk"
            ),
            F.avg("churn_risk_score").alias("avg_churn_risk"),
        )
        .withColumn(
            "approval_rate",
            F.round(F.col("approved_count") / F.col("applications_count"), 2),
        )
        .withColumn(
            "risk_segment",
            F.when(
                F.col("avg_risk_score") > 70,
                "high_risk",
            )
            .when(
                F.col("avg_risk_score") > 50,
                "medium_risk",
            )
            .otherwise("low_risk"),
        )
        .drop("approved_count")
    )


def process_batch(batch_df: DataFrame, batch_id: int):
    """
    Process a single batch of data from the Kafka topic.

    This function reads the customer data from Redis, joins it with the
    loan application data, analyzes the risk of the customers, and writes
    the analyzed data to Kafka.

    Args:
        batch_df (DataFrame): The DataFrame containing the loan applications for this batch.
        batch_id (int): The ID of the batch.

    """

    customer_ids = batch_df.select("customer_id").distinct().collect()
    customer_ids = [x.customer_id for x in customer_ids]

    redis_df = _read_single_customer_from_redis(
        spark=batch_df.sparkSession, customer_id=customer_ids[0]
    )

    final_df = batch_df.join(redis_df, on=["customer_id"], how="inner")

    risk_df = analyze_risk(final_df)

    to_kafka_df = risk_df.select(
        F.col("customer_id").alias("key"), F.to_json(F.struct("*")).alias("value")
    ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    write_to_kafka(to_kafka_df, settings.kafka_output_topic)


if __name__ == "__main__":
    spark: SparkSession = get_spark_session()

    df: DataFrame = read_kafka_stream(
        spark=spark, topic=settings.kafka_input_topic, schema=LoanApplicationSchema
    )
    df.writeStream.foreachBatch(process_batch).start().awaitTermination()
