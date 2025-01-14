# Configuration
KAFKA_CONTAINER=kafka
BOOTSTRAP_SERVER=kafka:9092
PARTITIONS=1
REPLICATION_FACTOR=1
KAFKA_CREATE_TOPICS_TIMEOUT=10

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
PIP=pip

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
			--partitions $(PARTITIONS) \
			--replication-factor $(REPLICATION_FACTOR) 2>&1 | grep -v "already exists" || true; \
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

.PHONY: all up down clean install test kafka-* spark-* load-redis help

# Default target
all: help

# Help command
help:
	@echo "Available commands:"
	@echo "Environment:"
	@echo "  make up            - Start all services"
	@echo "  make down          - Stop all services"
	@echo "  make clean         - Clean up temporary files and logs"
	@echo ""
	@echo "Kafka commands:"
	@echo "  make kafka-list    - List all Kafka topics"
	@echo "  make kafka-delete-all      - Delete all Kafka topics"
	@echo "  make kafka-create-all      - Create all Kafka topics"
	@echo "  make kafka-recreate-all    - Recreate all Kafka topics"
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



up:
	@echo "Starting Docker Compose services..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d
	@echo "Waiting for services to be ready..."
	sleep $(KAFKA_CREATE_TOPICS_TIMEOUT)
	$(MAKE) kafka-create-all

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
	find . -type d -name ".eggs" -exec rm -r {} +
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name "build" -exec rm -r {} +
	find . -type d -name "dist" -exec rm -r {} +

# Kafka topic management
kafka-list:
	@echo "Listing Kafka topics..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list

kafka-delete-all: kafka-delete-output-redis-topic kafka-delete-input-redis-topic kafka-delete-output-only-kafka-topics kafka-delete-input-only-kafka-topic

kafka-create-all: kafka-create-output-redis-topic kafka-create-input-redis-topic kafka-create-output-kafka-only-topics kafka-create-input-only-kafka-topic

kafka-recreate: kafka-delete-all kafka-create-all


# ------------------------------------------------------------------------------|
# Application with Redis:                                                        |
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
	@echo "Loading data into Redis..."
	$(PYTHON) -m redis_loader.loader

# kafka producer
kafka-producer-redis:
	@echo "Running Kafka producer with Redis..."
	$(PYTHON) -m kafka_producer.run_kafka_with_redis_data_producer

# spark app
spark-app-redis:
	@echo "Running Spark application with Redis..."
	$(PYTHON) -m sparky.app_kafka_with_redis


# ------------------------------------------------------------------------------|
# Application with Kafka Only:                                                   |
# ------------------------------------------------------------------------------|
kafka-create-input-only-kafka-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_INPUT_TOPIC))

kafka-create-output-kafka-only-topics:
	@echo "Creating output topics..."
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC))
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC))

kafka-delete-input-only-kafka-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_INPUT_TOPIC))

kafka-delete-output-only-kafka-topics:
	@echo "Deleting output topics..."
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
	@echo "Running Kafka producer with Kafka Only..."
	$(PYTHON) -m kafka_producer.run_kafka_only_data_producer

spark-app-kafka-only:
	@echo "Running Spark application with Kafka Only..."
	$(PYTHON) -m sparky.app_kafka_only