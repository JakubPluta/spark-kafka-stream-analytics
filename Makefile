# Configuration
KAFKA_CONTAINER=kafka
BOOTSTRAP_SERVER=kafka:9092
PARTITIONS=1
REPLICATION_FACTOR=1
KAFKA_CREATE_TOPICS_TIMEOUT=15

# Application settings
# kafka with redis project
APP_WITH_REDIS_INPUT_TOPIC=loan-application-events
APP_WITH_REDIS_OUTPUT_TOPIC=loan-application-events-processed
# only kafka project
APP_ONLY_KAFKA_INPUT_TOPIC=stream-loan-events
APP_ONLY_KAFKA_OUTPUT_TOPIC=stream-loan-events-processed
APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC=stream-loan-events-processed-risk
APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC=stream-loan-events-processed-fraud
APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC=stream-loan-events-processed-stats
APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC=stream-loan-events-processed-segment
APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC=stream-loan-events-processed-channel

# Docker and environment settings
DOCKER_COMPOSE_FILE=docker-compose.yaml
PYTHON=python


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
	$(MAKE) kafka-create

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

define topic_exists
	$(shell docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list | grep -x $(1) > /dev/null && echo 1 || echo 0)
endef

test_topic_exists:
	@echo "Topic exists: $(call topic_exists,$(APP_WITH_REDIS_OUTPUT_TOPIC))"


# Enhanced topic creation with configuration
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

# delete topic


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

kafka-create-input-redis-topic:
	$(call create_topic,$(APP_WITH_REDIS_INPUT_TOPIC))

kafka-create-output-redis-topic:
	$(call create_topic,$(APP_WITH_REDIS_OUTPUT_TOPIC))

kafka-create-input-single-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_INPUT_TOPIC))

kafka-create-output-single-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_TOPIC))

kafka-create-output-risk-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC))

kafka-create-output-fraud-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC))

kafka-create-output-stats-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC))

kafka-create-output-segment-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC))

kafka-create-output-channel-topic:
	$(call create_topic,$(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC))


kafka-delete-input-redis-topic:
	$(call delete_topic,$(APP_WITH_REDIS_INPUT_TOPIC))

kafka-delete-output-redis-topic:
	$(call delete_topic,$(APP_WITH_REDIS_OUTPUT_TOPIC))

kafka-delete-input-single-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_INPUT_TOPIC))

kafka-delete-output-single-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_TOPIC))

kafka-delete-output-risk-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC))

kafka-delete-output-fraud-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC))

kafka-delete-output-stats-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC))

kafka-delete-output-segment-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC))

kafka-delete-output-channel-topic:
	$(call delete_topic,$(APP_ONLY_KAFKA_OUTPUT_CHANNEL_TOPIC))

# Grouped commands
kafka-create: kafka-create-input-redis-topic kafka-create-output-redis-topic kafka-create-input-single-topic kafka-create-output-single-topic kafka-create-output-risk-topic kafka-create-output-fraud-topic kafka-create-output-stats-topic kafka-create-output-segment-topic

kafka-delete: kafka-delete-input-redis-topic kafka-delete-output-redis-topic kafka-delete-input-single-topic kafka-delete-output-single-topic kafka-delete-output-risk-topic kafka-delete-output-fraud-topic kafka-delete-output-stats-topic kafka-delete-output-segment-topic

kafka-recreate: kafka-delete kafka-create

# Consumer commands with enhanced options
define start_consumer
	@echo "Starting Kafka consumer for topic: $(1)..."
	docker exec $(KAFKA_CONTAINER) kafka-console-consumer \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--topic $(1) \
		$(2)
endef

kafka-consumer-input-redis:
	$(call start_consumer,$(APP_WITH_REDIS_INPUT_TOPIC),)

kafka-consumer-output-redis:
	$(call start_consumer,$(APP_WITH_REDIS_OUTPUT_TOPIC),)

kafka-consumer-input-redis-from-beginning:
	$(call start_consumer,$(APP_WITH_REDIS_INPUT_TOPIC),--from-beginning)

kafka-consumer-output-redis-from-beginning:
	$(call start_consumer,$(APP_WITH_REDIS_OUTPUT_TOPIC),--from-beginning)

kafka-consumer-input-single:
	$(call start_consumer,$(APP_ONLY_KAFKA_INPUT_TOPIC),)

kafka-consumer-input-single-from-beginning:
	$(call start_consumer,$(APP_ONLY_KAFKA_INPUT_TOPIC),--from-beginning)


kafka-consumer-output-risk:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_RISK_TOPIC),)

kafka-consumer-output-fraud:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_FRAUD_TOPIC),)

kafka-consumer-output-stats:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_STATS_TOPIC),)

kafka-consumer-output-segment:
	$(call start_consumer,$(APP_ONLY_KAFKA_OUTPUT_SEGMENT_TOPIC),)


# Application commands
load-redis:
	@echo "Loading data into Redis..."
	$(PYTHON) -m redis_loader.loader

kafka-producer-redis:
	@echo "Running Kafka producer with Redis..."
	$(PYTHON) -m kafka_producer.run_simple_producer

kafka-producer-single:
	@echo "Running Kafka producer without Redis..."
	$(PYTHON) -m kafka_producer.run_full_msg_producer

spark-app-redis:
	@echo "Running Spark application with Redis..."
	$(PYTHON) -m sparky.app

spark-app-single:
	@echo "Running Spark application without Redis..."
	$(PYTHON) -m sparky.app_kafka_only