# Configuration
KAFKA_CONTAINER=kafka
BOOTSTRAP_SERVER=kafka:9092
TOPIC=loan-application-events
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

# Start Kafka consumer for a specific topic
kafka-consumer:
	@echo "Starting Kafka consumer for topic: $(TOPIC)..."
	docker exec $(KAFKA_CONTAINER) kafka-console-consumer \
		--bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC)

kafka-consumer-from-beginning:
	@echo "Starting Kafka consumer for topic: $(TOPIC) from the beginning..."
	docker exec $(KAFKA_CONTAINER) kafka-console-consumer \
		--bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC) --from-beginning


# Recreate a Kafka topic
kafka-delete:
	@echo "Deleting Kafka topic: $(TOPIC)..."
	@if docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --list | grep -q $(TOPIC); then \
		docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --delete --topic $(TOPIC); \
		echo "Topic $(TOPIC) successfully deleted."; \
	else \
		echo "Topic $(TOPIC) does not exist, skipping delete."; \
	fi

kafka-create:
	@echo "Creating Kafka topic: $(TOPIC) with $(PARTITIONS) partitions and $(REPLICATION_FACTOR) replication factor..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(BOOTSTRAP_SERVER) --create --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor $(REPLICATION_FACTOR)

kafka-recreate: kafka-delete kafka-create
	@echo "Kafka topic $(TOPIC) has been deleted and recreated with $(PARTITIONS) partitions and $(REPLICATION_FACTOR) replication factor."


load-redis:
	@echo "Loading data into Redis..."
	python -m redis_loader.loader

kafka-producer:
	@echo "Running Kafka producer..."
	python -m kafka_producer.producer

spark-app:
	@echo "Running Spark application..."
	python -m sparky.app