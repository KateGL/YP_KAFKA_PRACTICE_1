import os
from confluent_kafka import Producer

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "python-producer",
        # другие настройки при необходимости
    }
    producer = Producer(conf)

    # Пример отправки
    topic = "test-topic"
    msg = b"hello from docker producer"
    producer.produce(topic, value=msg)
    producer.flush(timeout=10.0)

if __name__ == "__main__":
    main()
