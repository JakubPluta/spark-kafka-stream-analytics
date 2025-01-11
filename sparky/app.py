from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from core.config import settings
from pyspark.sql import functions as F

from sparky.schema import LoanApplicationSchema


def get_spark_session(app_name: str = "SparkStreamingApp"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"
        )
        .getOrCreate()
    )
    #spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, topic: str, schema: StructType):
    data = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(F.from_json(F.col("value"), schema).alias("data"))
        .select("data.*")
    )

    return data


if __name__ == "__main__":
    spark: SparkSession = get_spark_session()

    df = read_kafka_stream(
        spark=spark, topic=settings.kafka_input_topic, schema=LoanApplicationSchema
    )

    df.writeStream.format("console").outputMode("append").start().awaitTermination()
