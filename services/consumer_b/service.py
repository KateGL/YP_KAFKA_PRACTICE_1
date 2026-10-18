import os
import signal
import time
from collections import deque
from confluent_kafka import Consumer, KafkaException

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
BATCH_TIMEOUT_SEC = float(os.getenv("BATCH_TIMEOUT_SEC", "2.0"))

def process_batch(batch):
    """
    Здесь твоя пакетная логика.
    batch — список строк (message.value().decode()).
    Например:
      - записать в БД одним INSERT ... VALUES (...),
      - отправить в API батчем,
      - обработать через pandas и сохранить результат.
    """
    print(f"[group-b] Processing batch of {len(batch)} messages")
    for i, value in enumerate(batch):
        # Пример: просто печатаем, но в реальности тут тяжёлая логика
        print(f"  [{i}] {value[:80]}...")
    # Здесь может быть вызов pandas, агрегация, запись в БД и т.д.

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("CONSUMER_GROUP_ID", "group-b")
    topic = os.getenv("KAFKA_TOPIC", "test-topic")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # мы сами будем коммитить после обработки батча
        "session.timeout.ms": 10000,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    batch = deque()
    last_flush_time = time.time()
    shutdown = False

    def handle_sigterm(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    try:
        while not shutdown:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                # Если сообщений нет, проверяем таймаут батча
                now = time.time()
                if batch and (now - last_flush_time) >= BATCH_TIMEOUT_SEC:
                    flush_batch(consumer, batch)
                    last_flush_time = now
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            value = msg.value().decode("utf-8", errors="replace")
            batch.append((msg, value))

            # Если набрали нужный размер — сбрасываем батч
            if len(batch) >= BATCH_SIZE:
                flush_batch(consumer, batch)
                last_flush_time = time.time()

        # При завершении — докидываем остаток
        if batch:
            flush_batch(consumer, batch)

    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        consumer.close()
        print("[group-b] Consumer closed.")

def flush_batch(consumer, batch):
    """Обработать батч и закоммитить офсеты"""
    values = [v for _, v in batch]
    process_batch(values)

    # Коммит последнего сообщения в батче (упрощённо)
    last_msg = batch[-1][0]
    consumer.commit(last_msg, asynchronous=False)

    # Очищаем батч
    batch.clear()

if __name__ == "__main__":
    main()
