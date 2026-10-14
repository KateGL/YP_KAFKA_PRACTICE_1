.PHONY: up down reset

up:
	docker compose -f docker-compose.yaml up --build -d

down:
	docker compose -f docker-compose.yaml down

reset:
	docker compose -f docker-compose.yaml down -v

CONTAINER_NAME ?= kafka-1
BOOTSTRAP_SERVER ?= kafka-1:29092
PARTITIONS ?= 3
REPLICATION_FACTOR ?= 2

# create-topic: make create-topic TOPIC=test-topic
create-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Ошибка: не задано имя топика. Используйте TOPIC=имя_топика"; \
		exit 1; \
	fi
	docker exec "$(CONTAINER_NAME)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVER)" \
		--create \
		--topic "$(TOPIC)" \
		--partitions "$(PARTITIONS)" \
		--replication-factor "$(REPLICATION_FACTOR)"

# describe-topic: make describe-topic TOPIC=test-topic
describe-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Ошибка: не задано имя топика. Используйте TOPIC=имя_топика"; \
		exit 1; \
	fi
	docker exec "$(CONTAINER_NAME)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVER)" \
		--describe \
		--topic "$(TOPIC)"
