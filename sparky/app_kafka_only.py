from typing import Dict

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming import StreamingQuery
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
            "maxOffsetsPerTrigger", 1
        )  # Only read one message at a time, as we can't read multiple messages from redis later
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(F.from_json(F.col("value"), schema).alias("data"))
        .select("data.*")
    )


def create_risk_analytics_stream(df: DataFrame) -> DataFrame:
    """
    Creates a real-time risk analytics stream to monitor high-risk loan applications.

    This function analyzes loan applications for potential risks by monitoring:
    - High risk assessment scores (> 70)
    - Large loan amounts (> 50,000)
    - High debt-to-income ratios (> 0.4)

    The combination of these factors helps identify applications that might need
    additional review or pose higher risk to the institution.

    Args:
        df (DataFrame): Input streaming DataFrame with loan application data

    Returns:
        DataFrame: Filtered stream of high-risk applications

    Example Risk Alert:
        {
            "event_id": "uuid",
            "timestamp": "2024-01-12T10:00:00",
            "risk_category": "HIGH",
            "risk_assessment_score": 85,
            "credit_score": 620,
            "requested_amount": 150000,
            "debt_to_income_ratio": 0.6
        }
    """

    risk_analysis = df.select(
        "event_id",
        "timestamp",
        "customer_profile.risk_category",
        "automated_decision",
        "risk_assessment_score",
        "customer_profile.financial_profile.credit_score",
        "requested_amount",
        "customer_profile.financial_profile.debt_to_income_ratio",
    ).withWatermark("timestamp", "1 minute")

    return risk_analysis.filter(
        (F.col("risk_assessment_score") > 70)
        & (F.col("requested_amount") > 50000)
        & (F.col("debt_to_income_ratio") > 0.4)
    )


def create_fraud_detection_stream(df: DataFrame) -> DataFrame:
    """
    Creates a real-time fraud detection stream to identify suspicious loan applications.

    This function monitors applications for potential fraud indicators including:
    - Low identity verification scores (< 0.6)
    - Suspiciously fast application completion (< 120 seconds)
    - Failed or flagged fraud checks

    The analysis helps prevent fraudulent applications and protect both the institution
    and legitimate customers.

    Args:
        df (DataFrame): Input streaming DataFrame with loan application data

    Returns:
        DataFrame: Filtered stream of suspicious applications

    Example Fraud Alert:
        {
            "event_id": "uuid",
            "timestamp": "2024-01-12T10:00:00",
            "fraud_check_result": "FLAG",
            "identity_verification_score": 0.4,
            "application_completion_time": 90,
            "ip_address": "192.168.1.1",
            "geo_location": {"latitude": 40.7128, "longitude": -74.0060}
        }
    """
    fraud_detection = df.select(
        "event_id",
        "timestamp",
        "fraud_check_result",
        "identity_verification_score",
        "device_fingerprint",
        "ip_address",
        "customer_profile.customer_id",
        "geo_location",
        "application_completion_time"
    ).withWatermark("timestamp", "1 minute")

    return fraud_detection.filter(
        (F.col("identity_verification_score") < 0.6) |
        (F.col("application_completion_time") < 120) |
        (F.col("fraud_check_result").isin("FLAG", "FAIL"))
    )


def create_application_statistics_stream(df: DataFrame) -> DataFrame:
    """
    Creates a real-time application statistics stream to monitor loan application patterns.

    This function provides aggregated statistics in 5-minute windows with 1-minute slides,
    tracking:
    - Total application count by channel
    - Total and average requested amounts
    - Approval counts and rates

    These metrics help monitor the overall health of the loan application system
    and identify trends or issues in real-time.

    Args:
        df (DataFrame): Input streaming DataFrame with loan application data

    Returns:
        DataFrame: Windowed aggregations of application statistics

    Example Statistics:
        {
            "window": {"start": "2024-01-12T10:00:00", "end": "2024-01-12T10:05:00"},
            "application_channel": "MOBILE_APP",
            "total_applications": 150,
            "total_requested_amount": 5000000,
            "avg_requested_amount": 33333.33,
            "approved_count": 120
        }
    """
    return df.select(
        "timestamp",
        "application_channel",
        "requested_amount",
        "automated_decision",
        "customer_profile.customer_segment"
    ).withWatermark("timestamp", "5 minutes") \
        .groupBy(
        F.window("timestamp", "5 minutes", "1 minute"),
        "application_channel"
    ).agg(
        F.count("*").alias("total_applications"),
        F.sum("requested_amount").alias("total_requested_amount"),
        F.avg("requested_amount").alias("avg_requested_amount"),
        F.count(F.when(F.col("automated_decision") == "APPROVED", True)).alias("approved_count")
    )


def create_segment_analysis_stream(df: DataFrame) -> DataFrame:
    """
    Creates a real-time customer segment analysis stream to understand borrowing patterns.

    This function analyzes loan applications by customer segment, providing insights into:
    - Application volumes by segment and loan purpose
    - Average credit scores per segment
    - Average requested amounts by segment

    This analysis helps in understanding customer behavior and tailoring products
    to different customer segments.

    Args:
        df (DataFrame): Input streaming DataFrame with loan application data

    Returns:
        DataFrame: Windowed aggregations of segment-based metrics

    Example Analysis:
        {
            "window": {"start": "2024-01-12T10:00:00", "end": "2024-01-12T10:05:00"},
            "customer_segment": "Affluent",
            "loan_purpose": "MORTGAGE",
            "application_count": 50,
            "avg_credit_score": 780,
            "avg_requested_amount": 500000
        }
    """
    return df.select(
        "timestamp",
        "customer_profile.customer_segment",
        "requested_amount",
        "loan_purpose",
        "customer_profile.financial_profile.credit_score"
    ).withWatermark("timestamp", "5 minutes") \
        .groupBy(
        F.window("timestamp", "5 minutes", "1 minute"),
        "customer_segment",
        "loan_purpose"
    ).agg(
        F.count("*").alias("application_count"),
        F.avg("credit_score").alias("avg_credit_score"),
        F.avg("requested_amount").alias("avg_requested_amount")
    )


def create_channel_metrics_stream(df: DataFrame) -> DataFrame:
    """
    Creates a real-time channel performance metrics stream to monitor application channels.

    This function tracks various performance metrics for each application channel:
    - Average completion time
    - Average number of attempts
    - Approval rates
    - Application volumes

    These metrics help identify channel-specific issues and optimize the application
    process across different channels.

    Args:
        df (DataFrame): Input streaming DataFrame with loan application data

    Returns:
        DataFrame: Windowed aggregations of channel performance metrics

    Example Metrics:
        {
            "window": {"start": "2024-01-12T10:00:00", "end": "2024-01-12T10:05:00"},
            "application_channel": "WEB",
            "avg_completion_time": 450,
            "avg_attempts": 1.5,
            "approval_rate": 0.85,
            "total_applications": 200
        }
    """
    return df.select(
        "timestamp",
        "application_channel",
        "application_completion_time",
        "number_of_attempts",
        "automated_decision"
    ).withWatermark("timestamp", "5 minutes") \
        .groupBy(
        F.window("timestamp", "5 minutes", "1 minute"),
        "application_channel"
    ).agg(
        F.avg("application_completion_time").alias("avg_completion_time"),
        F.avg("number_of_attempts").alias("avg_attempts"),
        F.count(F.when(F.col("automated_decision") == "APPROVED", True)).alias("approved_count"),
        F.count("*").alias("total_applications")
    ).withColumn(
        "approval_rate",
        F.col("approved_count") / F.col("total_applications")
    )


def write_streaming_output_to_console(
        df: DataFrame,
        name: str,
        output_mode: str = "append"
) -> StreamingQuery:
    """
    Writes a streaming DataFrame to the console for monitoring.

    Args:
        df (DataFrame): The streaming DataFrame to write
        name (str): Name of the stream for identification
        output_mode (str): Output mode for the stream ("append" or "complete")

    Returns:
        StreamingQuery: The streaming query object
    """
    return df.writeStream \
        .queryName(name) \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", False) \
        .start()


def create_streaming_analytics(df: DataFrame) -> Dict[str, StreamingQuery]:
    """
    Creates multiple streaming analytics pipelines for loan application data.

    This function sets up five different types of analysis:
    1. Risk Analytics - Monitoring high-risk applications
    2. Fraud Detection - Identifying suspicious activities
    3. Application Statistics - Tracking overall application patterns
    4. Segment Analysis - Understanding customer segment behavior
    5. Channel Metrics - Monitoring channel performance

    Args:
        df (DataFrame): Input streaming DataFrame with loan application data

    Returns:
        Dict[str, StreamingQuery]: Dictionary of streaming queries
    """
    # Create individual analysis streams
    risk_alerts = create_risk_analytics_stream(df)
    suspicious_activities = create_fraud_detection_stream(df)
    application_stats = create_application_statistics_stream(df)
    segment_analysis = create_segment_analysis_stream(df)
    channel_metrics = create_channel_metrics_stream(df)

    # Write streams to output
    queries = {
        "risk_alerts": write_streaming_output_to_console(risk_alerts, "risk_alerts"),
        "suspicious_activities": write_streaming_output_to_console(suspicious_activities, "suspicious_activities"),
        "application_stats": write_streaming_output_to_console(application_stats, "application_stats", "complete"),
        "segment_analysis": write_streaming_output_to_console(segment_analysis, "segment_analysis", "complete"),
        "channel_metrics": write_streaming_output_to_console(channel_metrics, "channel_metrics", "complete")
    }

    return queries



if __name__ == "__main__":
    spark: SparkSession = get_spark_session()

    df: DataFrame = read_kafka_stream(
        spark=spark,
        topic=settings.kafka_with_redis_input_topic,
        schema=LoanApplicationSchemaWithCustomerProfile,
    )

    queries = create_streaming_analytics(df)

    # Wait for all queries to terminate
    for query in queries.values():
        query.awaitTermination()
