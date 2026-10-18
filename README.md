# YP_KAFKA_PRACTICE_1
Репо для сдачи Практической работы 1 по курсу ЯП Kafka


make reset          # полный сброс
make up-infra       # поднять Kafka
make create-topic TOPIC=test-topic
make up             # поднять всё вместе




# 1. Только Kafka + Zookeeper
make up-infra

# 2. Создать топик (теперь это безопасно: кластер уже работает)
make create-topic TOPIC=orders

# 3. Запустить продюсер и консьюмеры
make up
# или, если они уже запущены и нужно просто перезапустить:
make restart-apps


# 1. Принудительно остановить и удалить все контейнеры проекта
docker compose -f docker-compose.yaml down -v --remove-orphans

# 2. Если сеть всё равно висит — удаляем её вручную
docker network rm yp_kafka_practice_1_kafka-net 2>/dev/null || true
