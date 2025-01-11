



up:
	docker compose up -d

down:
	docker compose down

lkt:
	docker exec kafka kafka-topics --list --bootstrap-server localhost:9092