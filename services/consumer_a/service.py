import os
import sys
from confluent_kafka import Consumer
from message import MessagePayload

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    topic = os.getenv("TOPIC", "test-topic")
    group_id = os.getenv("CONSUMER_GROUP_ID", "group-single")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,   # коммит каждые 1 сек (или после N сообщений)
        "fetch.min.bytes": 1,             # можно брать даже 1 байт — для режима «по одному»

    }

    c = Consumer(conf)
    c.subscribe([topic])
    print(f"🚀 SingleMessageConsumer запущен. Группа: {group_id}, топик: {topic}")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Ошибка сообщения: {msg.error()}")
                continue

            try:
                payload = MessagePayload.from_json(msg.value().decode("utf-8"))
                print(f"📩 SingleMessageConsumer получил: {payload}")
                # здесь бизнес-логика обработки одного сообщения
            except Exception as e:
                print(f"❌ Ошибка сериализации/обработки: {e}")
                # продолжаем работать, не прерываем цикл
    except Exception as e:
        print(f"💥 Непредвиденная ошибка: {e}", file=sys.stderr)
    finally:
        print("🛑 Закрываем Consumer...")
        c.close()
        print("✅ Consumer закрыт.")

if __name__ == "__main__":
    main()
