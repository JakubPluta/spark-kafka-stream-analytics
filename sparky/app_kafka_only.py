from dataclasses import dataclass
from typing import Dict, Optional, Callable, List, Union

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from core.config import settings
from pyspark.sql import functions as F

from sparky.schema import (
    LoanApplicationSchemaWithCustomerProfile,
)
from pyspark.sql.streaming.readwriter import DataStreamReader
from enum import Enum
from core.logger import get_logger

logger = get_logger(__name__)


class KafkaOutputTopics:
    BASE_OUTPUT_TOPIC = "stream-loan-events-processed"
    RISK_OUTPUT_TOPIC = f"{BASE_OUTPUT_TOPIC}-risk"
    FRAUD_OUTPUT_TOPIC = f"{BASE_OUTPUT_TOPIC}-fraud"
    STATS_OUTPUT_TOPIC = f"{BASE_OUTPUT_TOPIC}-stats"
    SEGMENT_OUTPUT_TOPIC = f"{BASE_OUTPUT_TOPIC}-segment"
    CHANNEL_OUTPUT_TOPIC = f"{BASE_OUTPUT_TOPIC}-channel"


class OutputFormat(str, Enum):
    """Supported output formats for streaming data"""

    KAFKA = "kafka"
    CONSOLE = "console"


@dataclass
class KafkaOutputConfig:
    """Configuration for Kafka output"""

    bootstrap_servers: str
    topic: str
    checkpoint_location: str
    output_mode: str = "append"
    trigger_interval: Optional[str] = "1 minute"

    def __repr__(self):
        return (
            f"KafkaOutputConfig(bootstrap_servers='{self.bootstrap_servers}', "
            f"topic='{self.topic}', checkpoint_location='{self.checkpoint_location}', "
            f"output_mode='{self.output_mode}', trigger_interval='{self.trigger_interval}')"
        )


@dataclass
class ConsoleOutputConfig:
    """Configuration for console output"""

    output_mode: str = "append"
    truncate: bool = False


def get_spark_session(app_name: str = "SparkStreamingApp") -> SparkSession:
    """
    Returns a SparkSession configured for Spark Streaming.

    Args:
        app_name (str): The name of the application.

    Returns:
        SparkSession: The SparkSession.
    """

    return (
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
    try:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_brokers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 1)
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(F.from_json(F.col("value"), schema).alias("data"))
            .select("data.*")
        )
    except Exception as e:
        logger.error(f"Failed to read Kafka stream: {str(e)}")
        raise


def write_to_kafka_stream(
    df: DataFrame, config: KafkaOutputConfig, query_name: str
) -> StreamingQuery:
    """Writes a DataFrame to a Kafka stream


    Args:
        df (DataFrame): The DataFrame to write.
        config (KafkaOutputConfig): The Kafka output configuration.
        query_name (str): The name of the query.

    Returns:
        StreamingQuery: The streaming query object
    """
    try:
        return (
            df.selectExpr("to_json(struct(*)) AS value")
            .writeStream.format("kafka")
            .queryName(query_name)
            .option("kafka.bootstrap.servers", config.bootstrap_servers)
            .option("topic", f"{config.topic}-{query_name}")
            .option("checkpointLocation", f"{config.checkpoint_location}/{query_name}")
            .outputMode(config.output_mode)
            .trigger(processingTime=config.trigger_interval)
            .start()
        )
    except Exception as e:
        logger.error(f"Failed to write to Kafka: {str(e)}")
        raise


def write_streaming_output_to_console(
    df: DataFrame, name: str, output_mode: str = "append"
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
    return (
        df.writeStream.queryName(name)
        .outputMode(output_mode)
        .format("console")
        .option("truncate", False)
        .start()
    )


def create_risk_analytics_stream(
    df: DataFrame,
    risk_score_threshold: float = 65.0,
    amount_threshold: float = 50000.0,
    dti_threshold: float = 0.4,
) -> DataFrame:
    """Creates risk analytics stream with configurable thresholds

    Args:
        df (DataFrame): Input streaming DataFrame with loan application data
        risk_score_threshold (float, optional): Threshold for risk assessment score. Defaults to 70.0.
        amount_threshold (float, optional): Threshold for requested amount. Defaults to 50000.0.
        dti_threshold (float, optional): Threshold for debt-to-income ratio. Defaults to 0.4.

    Returns:
        DataFrame: Risk analytics stream
    """
    return (
        df.select(
            "event_id",
            "timestamp",
            "customer_profile.risk_category",
            "automated_decision",
            "risk_assessment_score",
            "customer_profile.financial_profile.credit_score",
            "requested_amount",
            "customer_profile.financial_profile.debt_to_income_ratio",
        )
        .withWatermark("timestamp", "1 minute")
        .filter(
            (F.col("risk_assessment_score") > risk_score_threshold)
            & (F.col("requested_amount") > amount_threshold)
            & (F.col("debt_to_income_ratio") > dti_threshold)
        )
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
        "application_completion_time",
    ).withWatermark("timestamp", "1 minute")

    return fraud_detection.filter(
        (F.col("identity_verification_score") < 0.6)
        | (F.col("application_completion_time") < 120)
        | (F.col("fraud_check_result").isin("FLAG", "FAIL"))
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
    return (
        df.select(
            "timestamp",
            "application_channel",
            "requested_amount",
            "automated_decision",
            "customer_profile.customer_segment",
        )
        .withWatermark("timestamp", "1 minutes")
        .groupBy(F.window("timestamp", "1 minutes", "30 seconds"), "application_channel")
        .agg(
            F.count("*").alias("total_applications"),
            F.sum("requested_amount").alias("total_requested_amount"),
            F.avg("requested_amount").alias("avg_requested_amount"),
            F.count(F.when(F.col("automated_decision") == "APPROVED", True)).alias(
                "approved_count"
            ),
        )
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
    return (
        df.select(
            "timestamp",
            "customer_profile.customer_segment",
            "requested_amount",
            "loan_purpose",
            "customer_profile.financial_profile.credit_score",
        )
        .withWatermark("timestamp", "1 minutes")
        .groupBy(
            F.window("timestamp", "1 minutes", "30 seconds"),
            "customer_segment",
            "loan_purpose",
        )
        .agg(
            F.count("*").alias("application_count"),
            F.avg("credit_score").alias("avg_credit_score"),
            F.avg("requested_amount").alias("avg_requested_amount"),
        )
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
    return (
        df.select(
            "timestamp",
            "application_channel",
            "application_completion_time",
            "number_of_attempts",
            "automated_decision",
        )
        .withWatermark("timestamp", "1 minutes")
        .groupBy(
            F.window("timestamp", "1 minutes", "30 seconds"), "application_channel"
        )
        .agg(
            F.avg("application_completion_time").alias("avg_completion_time"),
            F.avg("number_of_attempts").alias("avg_attempts"),
            F.count(F.when(F.col("automated_decision") == "APPROVED", True)).alias(
                "approved_count"
            ),
            F.count("*").alias("total_applications"),
        )
        .withColumn(
            "approval_rate", F.col("approved_count") / F.col("total_applications")
        )
    )


def write_stream(
    df: DataFrame,
    output_format: OutputFormat,
    query_name: str,
    output_config: Union[KafkaOutputConfig, ConsoleOutputConfig],
) -> StreamingQuery:
    """
    Write stream to specified output with flexible configuration
    """
    writer = df.writeStream.queryName(query_name)

    try:
        if output_format == OutputFormat.KAFKA:
            if not isinstance(output_config, KafkaOutputConfig):
                raise ValueError("KafkaOutputConfig required for Kafka output")

            logger.info(f"Starting Kafka stream: {query_name}")
            logger.info(f"Topic: {output_config.topic}")
            logger.info(f"Checkpoint: {output_config.checkpoint_location}")

            writer = (
                df.selectExpr("to_json(struct(*)) AS value")
                .writeStream.format("kafka")
                .option("kafka.bootstrap.servers", output_config.bootstrap_servers)
                .option("topic", output_config.topic)
                .option(
                    "checkpointLocation",
                    f"{output_config.checkpoint_location}/{query_name}",
                )
                .outputMode(output_config.output_mode)
                .trigger(processingTime=output_config.trigger_interval)
            )

        elif output_format == OutputFormat.CONSOLE:
            if not isinstance(output_config, ConsoleOutputConfig):
                raise ValueError("ConsoleOutputConfig required for console output")

            logger.info(f"Starting Console stream: {query_name}")

            writer = (
                df.writeStream.format("console")
                .option("truncate", output_config.truncate)
                .outputMode(output_config.output_mode)
            )

        query = writer.start()
        logger.info(f"Successfully started stream: {query_name}")
        return query

    except Exception as e:
        logger.error(f"Failed to start stream {query_name}: {str(e)}")
        raise


def main():
    """Main application entry point"""
    try:
        logger.info("Initializing Spark session...")
        spark: SparkSession = get_spark_session()

        output_format = OutputFormat.KAFKA

        logger.info("Reading input stream...")
        input_stream = read_kafka_stream(
            spark,
            settings.kafka_only_input_topic,
            LoanApplicationSchemaWithCustomerProfile,
        )

        # Define analytics streams
        logger.info("Creating analytics streams...")
        analytics_streams = {
            "risk_analytics": create_risk_analytics_stream(input_stream),
            "fraud_detection": create_fraud_detection_stream(input_stream),
            "application_stats": create_application_statistics_stream(input_stream),
            "segment_analysis": create_segment_analysis_stream(input_stream),
            "channel_metrics": create_channel_metrics_stream(input_stream),
        }

        # Define output topics
        kafka_configs = {
            "risk_analytics": KafkaOutputConfig(
                bootstrap_servers=settings.kafka_brokers,
                topic=KafkaOutputTopics.RISK_OUTPUT_TOPIC,
                checkpoint_location=f"{settings.checkpoint_location}/risk",
            ),
            "fraud_detection": KafkaOutputConfig(
                bootstrap_servers=settings.kafka_brokers,
                topic=KafkaOutputTopics.FRAUD_OUTPUT_TOPIC,
                checkpoint_location=f"{settings.checkpoint_location}/fraud",
            ),
            "application_stats": KafkaOutputConfig(
                bootstrap_servers=settings.kafka_brokers,
                topic=KafkaOutputTopics.STATS_OUTPUT_TOPIC,
                checkpoint_location=f"{settings.checkpoint_location}/stats",
            ),
            "segment_analysis": KafkaOutputConfig(
                bootstrap_servers=settings.kafka_brokers,
                topic=KafkaOutputTopics.SEGMENT_OUTPUT_TOPIC,
                checkpoint_location=f"{settings.checkpoint_location}/segment",
            ),
            "channel_metrics": KafkaOutputConfig(
                bootstrap_servers=settings.kafka_brokers,
                topic=KafkaOutputTopics.CHANNEL_OUTPUT_TOPIC,
                checkpoint_location=f"{settings.checkpoint_location}/channel",
            ),
        }
        logger.info("Configured Kafka configs: {}".format(kafka_configs))
        console_config = ConsoleOutputConfig(output_mode="append", truncate=False)

        logger.info("Starting all streams...")
        queries = []

        # Start each stream individually with better error handling
        for stream_name, stream_df in analytics_streams.items():
            try:
                if output_format == OutputFormat.KAFKA:
                    logger.info(f"Starting Console stream for {stream_name}")
                    kafka_query = write_stream(
                        stream_df,
                        OutputFormat.KAFKA,
                        f"{stream_name}_kafka",
                        kafka_configs[stream_name],
                    )
                    queries.append(kafka_query)
                else:
                    # Console output
                    logger.info(f"Starting Console stream for {stream_name}")
                    console_query = write_stream(
                        stream_df,
                        OutputFormat.CONSOLE,
                        f"{stream_name}_console",
                        console_config,
                    )
                    queries.append(console_query)

            except Exception as e:
                logger.error(f"Failed to start {stream_name} streams: {str(e)}")
                # Continue with other streams even if one fails
                continue

        logger.info(f"Successfully started {len(queries)} streams")

        # Wait for termination
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
