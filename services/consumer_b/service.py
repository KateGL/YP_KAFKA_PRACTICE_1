import os
import sys
import signal
from confluent_kafka import Consumer
from message import MessagePayload

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    topic = os.getenv("TOPIC", "test-topic")
    group_id = os.getenv("CONSUMER_GROUP_ID", "group-batch")
    # Минимальный размер батча: сколько сообщений хотим накопить перед обработкой
    min_batch_size = int(os.getenv("MIN_BATCH_SIZE", "10"))

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    c = Consumer(conf)
    c.subscribe([topic])
    print(f"🚀 BatchMessageConsumer запущен. Группа: {group_id}, топик: {topic}, min_batch_size={min_batch_size}")

    accumulated_batch = []
    stop_requested = False

    def handle_stop_signal(signum, frame):
        nonlocal stop_requested
        print(f"\n⚠️ Получен сигнал {signum} — инициируем корректную остановку...")
        stop_requested = True

    # Регистрируем обработчики SIGTERM и SIGINT
    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    try:
        while not stop_requested:
            msgs = c.consume(num_messages=100, timeout=2.0)  # получаем до 100 сообщений
            if not msgs:
                continue

            for msg in msgs:
                if msg.error():
                    print(f"⚠️ Ошибка сообщения: {msg.error()}")
                    continue

                try:
                    payload = MessagePayload.from_json(msg.value().decode("utf-8"))
                    accumulated_batch.append((msg, payload))
                except Exception as e:
                    print(f"❌ Ошибка десериализации: {e}")
                    # пропускаем это сообщение, но продолжаем обработку остальных

            # обрабатываем пачку
            if len(accumulated_batch) >= min_batch_size:
                for msg, payload in accumulated_batch:
                    print(f"📩 BatchMessageConsumer обработал: {payload}")
                    # бизнес-логика для каждого сообщения

                # Коммит всей пачки одним вызовом
                c.commit(asynchronous=False)
                print(f"💾 BatchMessageConsumer сделал коммит после обработки {len(accumulated_batch)} сообщений")
                # Очищаем накопленный батч
                accumulated_batch.clear()

        # Цикл завершился по stop_requested — обрабатываем остаток
        if accumulated_batch:
            print(f"📦 Обрабатываем оставшиеся {len(accumulated_batch)} сообщений перед остановкой...")
            for msg, payload in accumulated_batch:
                print(f"📩 BatchMessageConsumer обработал (остаток): {payload}")
                # Бизнес-логика

            c.commit(asynchronous=False)
            print("💾 Коммит остатка выполнен.")


    except Exception as e:
        print(f"💥 Непредвиденная ошибка: {e}", file=sys.stderr)
    finally:
        print("🛑 Закрываем Consumer...")
        c.close()
        print("✅ Consumer закрыт.")

if __name__ == "__main__":
    main()
