import os
from confluent_kafka import Consumer
from message import MessagePayload

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    topic = os.getenv("TOPIC", "test-topic")
    group_id = os.getenv("CONSUMER_GROUP_ID", "group-batch")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,       # отключаем автокоммит
        "fetch.min.bytes": 10240,         # минимум 10 КБ — это обычно >10 сообщений (подбирается экспериментально)
    }

    c = Consumer(conf)
    c.subscribe([topic])
    print(f"🚀 BatchMessageConsumer запущен. Группа: {group_id}, топик: {topic}")

    try:
        while True:
            msgs = c.consume(num_messages=100, timeout=2.0)  # получаем до 100 сообщений
            if not msgs:
                continue

            batch = []
            for msg in msgs:
                if msg.error():
                    print(f"⚠️ Ошибка сообщения в батче: {msg.error()}")
                    continue
                try:
                    payload = MessagePayload.from_json(msg.value().decode("utf-8"))
                    batch.append((msg, payload))
                except Exception as e:
                    print(f"❌ Ошибка десериализации в батче: {e}")
                    # пропускаем это сообщение, но продолжаем обработку остальных

            # обрабатываем пачку
            for msg, payload in batch:
                print(f"📩 BatchMessageConsumer обработал: {payload}")
                # бизнес-логика для каждого сообщения

            # один коммит для всей пачки (гарантирует, что обработанные сообщения не будут прочитаны повторно)
            c.commit(asynchronous=False)
            print(f"💾 BatchMessageConsumer сделал коммит после обработки {len(batch)} сообщений")

    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()
