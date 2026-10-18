import os
import signal
import sys
from confluent_kafka import Consumer, KafkaException

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("CONSUMER_GROUP_ID", "group-a")
    topic = os.getenv("KAFKA_TOPIC", "test-topic")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 10000,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    shutdown = False

    def handle_sigterm(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    try:
        while not shutdown:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Для простых случаев можно просто логировать и продолжать
                print(f"Consumer error: {msg.error()}")
                continue

            value = msg.value().decode("utf-8", errors="replace")
            print(f"[group-a] Received: {value} | offset={msg.offset()} | partition={msg.partition()}")
            # Здесь твоя бизнес-логика на одно сообщение
            # Например: сохранить в БД, отправить в сервис и т.п.
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        consumer.close()
        print("[group-a] Consumer closed.")

if __name__ == "__main__":
    main()
