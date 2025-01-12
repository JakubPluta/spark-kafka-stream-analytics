# Configuration
KAFKA_CONTAINER=kafka
BOOTSTRAP_SERVER=kafka:9092
TOPIC=loan-application-events
OUTPUT_TOPIC=loan-application-events-processed
PARTITIONS=3
REPLICATION_FACTOR=1

# Bring up services
up:
	@echo "Starting Docker Compose services..."
	docker compose up -d

# Bring down services
down:
	@echo "Stopping Docker Compose services..."
	docker compose down

# List all Kafka topics
kafka-list:
	@echo "Listing Kafka topics..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list

kafka-delete-all:
	@echo "Deleting all Kafka topics..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --delete --topic '.*'

# Start Kafka consumer for a specific topic
kafka-consumer:
	@echo "Starting Kafka consumer for topic: $(TOPIC)..."
	docker exec $(KAFKA_CONTAINER) kafka-console-consumer \
		--bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC)

kafka-consumer-from-beginning:
	@echo "Starting Kafka consumer for topic: $(TOPIC) from the beginning..."
	docker exec $(KAFKA_CONTAINER) kafka-console-consumer \
		--bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC) --from-beginning



kafka-delete-input-topic:
	@echo "Deleting Kafka topic: $(TOPIC)..."
	@if docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list | grep -q $(TOPIC); then \
		docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --delete --topic $(TOPIC); \
		echo "Topic $(TOPIC) successfully deleted."; \
	else \
		echo "Topic $(TOPIC) does not exist, skipping delete."; \
	fi


kafka-create-input-topic:
	@echo "Creating Kafka topic: $(TOPIC) with $(PARTITIONS) partitions and $(REPLICATION_FACTOR) replication factor..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --create --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor $(REPLICATION_FACTOR)

kafka-delete-output-topic:
	@echo "Deleting Kafka topic: $(OUTPUT_TOPIC)..."
	@if docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list | grep -q $(OUTPUT_TOPIC); then \
		docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --delete --topic $(OUTPUT_TOPIC); \
		echo "Topic $(OUTPUT_TOPIC) successfully deleted."; \
	else \
		echo "Topic $(OUTPUT_TOPIC) does not exist, skipping delete."; \
	fi

kafka-create-output-topic:
	@echo "Creating Kafka topic: $(OUTPUT_TOPIC) with $(PARTITIONS) partitions and $(REPLICATION_FACTOR) replication factor..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --create --topic $(OUTPUT_TOPIC) --partitions $(PARTITIONS) --replication-factor $(REPLICATION_FACTOR)


kafka-delete: kafka-delete-input-topic kafka-delete-output-topic
	@echo "Kafka topics $(TOPIC) and $(OUTPUT_TOPIC) have been deleted."

kafka-create: kafka-create-input-topic kafka-create-output-topic
	@echo "Kafka topics $(TOPIC) and $(OUTPUT_TOPIC) have been created with $(PARTITIONS) partitions and $(REPLICATION_FACTOR) replication factor."


kafka-recreate: kafka-delete kafka-create
	@echo "Kafka topics $(TOPIC) and $(OUTPUT_TOPIC) have been recreated with $(PARTITIONS) partitions and $(REPLICATION_FACTOR) replication factor."


load-redis:
	@echo "Loading data into Redis..."
	python -m redis_loader.loader

kafka-producer:
	@echo "Running Kafka producer..."
	python -m kafka_producer.producer

spark-app:
	@echo "Running Spark application..."
	python -m sparky.app