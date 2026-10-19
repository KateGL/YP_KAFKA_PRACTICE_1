import os
import time
import random
from confluent_kafka import Producer
from message import MessagePayload

container_name = os.getenv("HOSTNAME", "unknown-producer")

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Сообщение не доставлено: {err}")
    else:
        print(f"✅ Сообщение доставлено в топик {msg.topic()} [{msg.partition()}] offset={msg.offset()}")

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    topic = os.getenv("TOPIC", "test-topic")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",                # гарантия At Least Once: ждём подтверждения от всех реплик
        "retries": 5,                # повторные попытки при временных сбоях
        "enable.idempotence": True,  # дополнительно защищает от дубликатов при ретраях (если брокер поддерживает)
        "message.timeout.ms": 30000,
    }

    p = Producer(conf)
    print(f"🚀 Продюсер запущен. Топик: {topic}, bootstrap: {bootstrap_servers}")

    msg_id = 0
    while True:
        msg_id += 1
        payload = MessagePayload(
            id=msg_id,
            source=f"producer: {container_name}",
            payload={"value": random.randint(1, 1000)},
            metadata={"timestamp": time.time()}
        )
        data = payload.to_json()
        print(f"📤 Отправка сообщения: {data}")

        # асинхронная отправка с callback
        p.produce(topic, value=data, callback=delivery_report)
        p.poll(0)  # запускаем callback-ы в фоне

        time.sleep(1)

if __name__ == "__main__":
    main()
