# Configuration
KAFKA_CONTAINER=kafka
BOOTSTRAP_SERVER=kafka:9092
PARTITIONS=1
REPLICATION_FACTOR=1
KAFKA_CREATE_TOPICS_TIMEOUT=15

# Application settings
# kafka with redis project
APP_WITH_REDIS_INPUT_TOPIC=loan-application-with-redis-events
APP_WITH_REDIS_OUTPUT_TOPIC=loan-application-with-redis-events-processed


# only kafka project
APP_ONLY_KAFKA_INPUT_TOPIC=stream-loan-events
APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC=stream-loan-events-processed-risk
APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC=stream-loan-events-processed-fraud
APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC=stream-loan-events-processed-stats
APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC=stream-loan-events-processed-segment
APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC=stream-loan-events-processed-channel

# Docker and environment settings
DOCKER_COMPOSE_FILE=docker-compose.yaml
PYTHON=python

# functions
define topic_exists
	$(shell docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list | grep -x $(1) > /dev/null && echo 1 || echo 0)
endef

define create_topic
	@echo "Creating/verifying Kafka topic: $(1)..."
	@if [ $(call topic_exists,$(1)) = 1 ]; then \
		echo "Topic $(1) already exists, skipping creation."; \
	else \
		docker exec $(KAFKA_CONTAINER) kafka-topics \
			--bootstrap-server $(BOOTSTRAP_SERVER) \
			--create \
			--topic $(1) \
			--partitions $(PARTITIONS) 2>&1 | grep -v "already exists" || true; \
	fi
endef

define delete_topic
	@echo "Checking and deleting Kafka topic: $(1)..."
	@if [ $(call topic_exists,$(1)) = 1 ]; then \
		echo "Deleting topic $(1)..."; \
		docker exec $(KAFKA_CONTAINER) kafka-topics \
			--bootstrap-server $(BOOTSTRAP_SERVER) \
			--delete \
			--topic $(1) || \
		(echo "Failed to delete topic $(1)" && exit 1); \
	else \
		echo "Topic $(1) does not exist, skipping deletion."; \
	fi
endef

# Consumer commands with enhanced options
define start_consumer
	@echo "Starting Kafka consumer for topic: $(1)..."
	docker exec $(KAFKA_CONTAINER) kafka-console-consumer \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--topic $(1) \
		$(2)
endef

.PHONY: all up down clean kafka-* spark-* load-redis help

# Default target
all: help

# Help command
help:
	@echo "Available commands:"
	@echo "Environment:"
	@echo "  make install        - Install Python dependencies"
	@echo "  make up            - Start all services"
	@echo "  make down          - Stop all services"
	@echo "  make clean         - Clean up temporary files and logs"
	@echo ""
	@echo "Kafka commands:"
	@echo "  make kafka-list    - List all Kafka topics"
	@echo "  make kafka-delete-all      - Delete all Kafka topics"
	@echo "  make kafka-recreate        - Recreate all Kafka topics"
	@echo "  make kafka-consumer-input-redis     - Start consumer for Redis input topic"
	@echo "  make kafka-consumer-output-redis    - Start consumer for Redis output topic"
	@echo ""
	@echo "Application commands:"
	@echo "  make load-redis            - Load data into Redis"
	@echo "  make kafka-producer-redis  - Run Kafka producer with Redis"
	@echo "  make kafka-producer-single - Run Kafka producer without Redis"
	@echo "  make spark-app-redis       - Run Spark application with Redis"
	@echo "  make spark-app-single      - Run Spark application without Redis"



# Environment management
up:
	@echo "Starting Docker Compose services..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d
	@echo "Waiting for services to be ready..."
	sleep 5
	#$(MAKE) kafka-create

down:
	@echo "Stopping Docker Compose services..."
	docker compose -f $(DOCKER_COMPOSE_FILE) down

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.log" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -r {} +

# Kafka topic management
kafka-list:
	@echo "Listing Kafka topics..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list

kafka-delete-all:
	@echo "Deleting all Kafka topics..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --delete --topic '.*' || true

# ------------------------------------------------------------------------------|
# Application with Redis:														|
# ------------------------------------------------------------------------------|
kafka-create-input-redis-topic:
	$(call create_topic,$(APP_WITH_REDIS_INPUT_TOPIC))

kafka-create-output-redis-topic:
	$(call create_topic,$(APP_WITH_REDIS_OUTPUT_TOPIC))

kafka-delete-input-redis-topic:
	$(call delete_topic,$(APP_WITH_REDIS_INPUT_TOPIC))

kafka-delete-output-redis-topic:
	$(call delete_topic,$(APP_WITH_REDIS_OUTPUT_TOPIC))

kafka-consumer-input-redis:
	$(call start_consumer,$(APP_WITH_REDIS_INPUT_TOPIC),)

kafka-consumer-output-redis:
	$(call start_consumer,$(APP_WITH_REDIS_OUTPUT_TOPIC),)

# group commands
kafka-create-kafka-redis: kafka-create-input-redis-topic kafka-create-output-redis-topic
kafka-delete-kafka-redis: kafka-delete-input-redis-topic kafka-delete-output-redis-topic
kafka-recreate-kafka-redis: kafka-delete-kafka-redis kafka-create-kafka-redis

# redis data loader - use at the beginning
load-redis:
	@echo "Loading data into Redis... redis_loader.loader.py"
	$(PYTHON) -m redis_loader.loader

# kafka producer
kafka-producer-redis:
	@echo "Running Kafka producer with Redis... kafka_producer.run_kafka_with_redis_data_producer.py"
	$(PYTHON) -m kafka_producer.run_kafka_with_redis_data_producer

# spark app
spark-app-redis:
	@echo "Running Spark application with Redis... sparky.app_kafka_with_redis.py"
	$(PYTHON) -m sparky.app_kafka_with_redis


# ------------------------------------------------------------------------------|
# Application with Kafka Only:													|
# ------------------------------------------------------------------------------|
kafka-create-input-only-kafka-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_INPUT_TOPIC))

kafka-create-output-kafka-only-topics:
	@echo "Creating ... $(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC)"
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC))


kafka-delete-input-only-kafka-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_INPUT_TOPIC))

kafka-delete-output-only-kafka-topics:
	@echo "Deleting Kafka topics... $(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC) $(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC)"
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC))
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC))
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC))
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC))
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC))


kafka-create-kafka-only: kafka-create-input-only-kafka-topic kafka-create-output-kafka-only-topics
kafka-delete-kafka-only: kafka-delete-input-only-kafka-topic kafka-delete-output-only-kafka-topics
kafka-recreate-kafka-only: kafka-delete-kafka-only kafka-create-kafka-only

kafka-consumer-input-only-kafka:
	$(call start_consumer,$(APP_ONLY_KAFKA_INPUT_TOPIC),)

kafka-consumer-output-only-kafka-risk:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC),)

kafka-consumer-output-only-kafka-fraud:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC),)

kafka-consumer-output-only-kafka-stats:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC),)

kafka-consumer-output-only-kafka-segment:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC),)

kafka-consumer-output-only-kafka-channel:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC),)

# application
kafka-producer-kafka-only:
	@echo "Running Kafka producer with Kafka Only: kafka_producer.run_kafka_only_data_producer."
	$(PYTHON) -m kafka_producer.run_kafka_only_data_producer

spark-app-kafka-only:
	@echo "Running Spark application with Kafka Only: sparky.app_kafka_only.py"
	$(PYTHON) -m sparky.app_kafka_only