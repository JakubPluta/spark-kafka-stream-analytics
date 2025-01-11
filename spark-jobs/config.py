import os


class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle")
    GPS_TOPIC = os.getenv("GPS_TOPIC", "gps")
    TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic")
    WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather")
    EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency")

    S3_BUCKET = os.getenv("S3_BUCKET", "spark-streaming")
    S3_CHECKPOINT_PREFIX = os.getenv("S3_CHECKPOINT_PREFIX", "checkpoints")

    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")

    S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", "data/output")
