class Settings:
    redis_host: str = "localhost"
    redis_port: int = 6379
    num_customers: int = 100_000

    kafka_brokers: str = "localhost:9092"
    kafka_with_redis_input_topic: str = "loan-application-with-redis-events"
    kafka_with_redis_output_topic: str = "loan-application-with-redis-events-processed"

    kafka_only_input_topic: str = "stream-loan-events"
    checkpoint_location: str = "/tmp/checkpoints"


settings = Settings()
