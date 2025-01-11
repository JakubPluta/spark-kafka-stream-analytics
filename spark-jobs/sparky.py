from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql import DataFrame, DataFrameWriter, DataFrameReader

VEHICLE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("model", StringType(), True),
    StructField("make", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("fuel", StringType(), True),
])

GPS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicle_type", StringType(), True),
])


TRAFFIC_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("camera_id", StringType(), True),
    StructField("snapshot", StringType(), True),
])


EMERGENCY_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True),
])

WEATHER_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("air_quality_index", DoubleType(), True),
])


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
        .withWatermark(
            'timestamp', '60 seconds'
        )
    )


def write_kafka_stream_to_parquet(input_df: DataFrame | DataFrameReader, checkpoint_location, output_path):
    return (
        input_df.writeStream
        .format('parquet')
        .option('checkpointLocation', checkpoint_location)
        .option('path', output_path)
        .outputMode('append')
        .start()
)

def write_kafka_stream_to_console(input_df: DataFrame | DataFrameReader):
    return (
        input_df.writeStream
        .format('console')
        .outputMode('append')
        .start()
)

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
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
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

    vehicle_df = read_kafka_stream_from_topic(spark, 'vehicle', VEHICLE_SCHEMA)
    gps_df = read_kafka_stream_from_topic(spark, 'gps', GPS_SCHEMA)
    traffic_df = read_kafka_stream_from_topic(spark, 'traffic', TRAFFIC_SCHEMA)
    weather_df = read_kafka_stream_from_topic(spark, 'weather', WEATHER_SCHEMA)
    emergency_df = read_kafka_stream_from_topic(spark, 'emergency', EMERGENCY_SCHEMA)

    # vehicle_df.writeStream.outputMode("append").format("console").start().awaitTermination()
    #
    # write_kafka_stream_to_parquet(
    #     vehicle_df, 'checkpoint/vehicle', 's3a://minio-vehicle-output/vehicle'
    # ).awaitTermination()

if __name__ == "__main__":
    main()
