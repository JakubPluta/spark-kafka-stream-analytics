class Settings:
    redis_host: str = "localhost"
    redis_port: int = 6379
    num_customers: int = 100_000

    kafka_brokers: str = "localhost:9092"
    kafka_input_topic: str = "loan_application_events"
    kafka_output_topic: str = "loan_application_events_processed"


settings = Settings()
