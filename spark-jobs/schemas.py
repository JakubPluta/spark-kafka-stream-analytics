from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)


VEHICLE_SCHEMA = StructType(
    [
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
    ]
)

GPS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True),
    ]
)


TRAFFIC_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("camera_id", StringType(), True),
        StructField("snapshot", StringType(), True),
    ]
)


EMERGENCY_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

WEATHER_SCHEMA = StructType(
    [
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
    ]
)
