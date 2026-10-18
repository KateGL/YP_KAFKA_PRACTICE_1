.PHONY: up up-infra down reset status create-topic describe-topic list-topics delete-topic restart-apps

# Явно указываем имя файла — так точно не будет ошибок с пустым именем
COMPOSE_FILE := docker-compose.yaml

CONTAINER_PREFIX := kafka-
BOOTSTRAP_SERVERS := kafka-1:9092,kafka-2:9092,kafka-3:9092

PARTITIONS ?= 3
REPLICATION_FACTOR ?= 3

# Получаем ID контейнера kafka-1
KAFKA_CONTAINER := $(shell docker compose -f $(COMPOSE_FILE) ps -q kafka-1 2>/dev/null)

check-cluster:
	@if [ -z "$(KAFKA_CONTAINER)" ]; then \
		echo "❌ Ошибка: Кластер Kafka не найден или не запущен."; \
		echo "   Запустите: make up-infra или make up"; \
		exit 1; \
	fi

up:
	@echo "🚀 Запуск всего (Kafka + продюсер + консьюмеры)..."
	docker compose -f $(COMPOSE_FILE) up --build -d

up-infra:
	@echo "🚀 Запуск только инфраструктуры Kafka (без продюсера и консьюмеров)..."
	docker compose -f $(COMPOSE_FILE) up --build -d kafka-1 kafka-2 kafka-3 zookeeper-1

down:
	@echo "🛑 Остановка всего кластера и приложений..."
	docker compose -f $(COMPOSE_FILE) down

reset:
	@echo "💥 Полный сброс (удаление контейнеров и volumes)..."
	docker compose -f $(COMPOSE_FILE) down -v

restart-apps:
	@echo "🔄 Перезапуск только продюсера и консьюмеров (Kafka остаётся работать)..."
	docker compose -f $(COMPOSE_FILE) restart producer consumer_a consumer_b

status:
	@echo "📊 Статус контейнеров Kafka:"
	docker ps --filter "name=$(CONTAINER_PREFIX)" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "🌐 Bootstrap Servers: $(BOOTSTRAP_SERVERS)"
	@echo "   Default Partitions: $(PARTITIONS)"
	@echo "   Default Replication Factor: $(REPLICATION_FACTOR)"
	@if [ -n "$(KAFKA_CONTAINER)" ]; then echo "✅ Кластер активен."; else echo "⚠️ Кластер не активен."; fi

create-topic: check-cluster
	@if [ -z "$(TOPIC)" ]; then \
		echo "❌ Ошибка: не задано имя топика. Используйте: make create-topic TOPIC=my-topic"; \
		exit 1; \
	fi
	@echo "✨ Создание топика '$(TOPIC)'..."
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--create \
		--topic "$(TOPIC)" \
		--partitions "$(PARTITIONS)" \
		--replication-factor "$(REPLICATION_FACTOR)" \
		--if-not-exists

describe-topic: check-cluster
	@if [ -z "$(TOPIC)" ]; then \
		echo "❌ Ошибка: не задано имя топика."; \
		exit 1; \
	fi
	@echo "👁️ Описание топика '$(TOPIC)'..."
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--describe \
		--topic "$(TOPIC)"

list-topics: check-cluster
	@echo "📋 Список топиков:"
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--list

delete-topic: check-cluster
	@if [ -z "$(TOPIC)" ]; then \
		echo "❌ Ошибка: не задано имя топика."; \
		exit 1; \
	fi
	@echo "⚠️ Внимание: удаление топика может не сработать, если delete.topic.enable=false в конфиге Kafka."
	@echo "   Удаление топика '$(TOPIC)'..."
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--delete \
		--topic "$(TOPIC)"
