import os
import signal
import sys
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
    message_posting_interval = int(os.getenv("MESSAGE_POSTING_INTERVAL", 1))

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
    stop_requested = False

    def handle_stop_signal(signum, frame):
        nonlocal stop_requested
        print(f"\n⚠️ Получен сигнал {signum} — инициируем корректную остановку продюсера...", file=sys.stderr)
        stop_requested = True

    # Регистрируем SIGTERM (от Docker) и SIGINT (Ctrl+C)
    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    try:
        while not stop_requested:
            msg_id += 1
            try:
                payload = MessagePayload(
                    id=msg_id,
                    source=f"producer: {container_name}",
                    payload={"value": random.randint(1, 1000)},
                    metadata={"timestamp": time.time()}
                )
                data = payload.to_json()
            except Exception as e:
                # Не даём упасть всему продюсеру из-за ошибки сериализации
                print(f"❌ Ошибка сериализации сообщения id={msg_id}: {e}", file=sys.stderr)
                # Пропускаем это сообщение, продолжаем дальше
                continue

            print(f"📤 Отправка сообщения: id={msg_id}")

            try:
                p.produce(topic, value=data, callback=delivery_report)
            except BufferError as e:
                # Буфер переполнен: ждём, пока освободится место
                print(f"⚠️ Буфер продюсера переполнен, ждём освобождения: {e}", file=sys.stderr)
                p.poll(0.5)  # ждём немного, чтобы callback-ы обработали часть сообщений
                # Повторяем попытку отправки в следующей итерации (msg_id уже увеличен, сообщение будет другим)
                continue

            # Регулярно вызываем poll, чтобы callback-ы отрабатывали
            p.poll(0)

            time.sleep(message_posting_interval)

        print("🛑 Цикл отправки завершён — выполняем flush() для досылки оставшихся сообщений...", file=sys.stderr)

    except Exception as e:
        print(f"💥 Непредвиденная ошибка в основном цикле: {e}", file=sys.stderr)
    finally:
        # flush() блокирует до тех пор, пока все сообщения не будут отправлены или не истечёт таймаут
        flush_timeout_sec = 10.0
        print(f"🛑 Выполняем flush() с таймаутом {flush_timeout_sec} сек...", file=sys.stderr)
        remaining = p.flush(timeout=flush_timeout_sec)
        if remaining > 0:
            print(f"⚠️ После flush() осталось неотправленными {remaining} сообщений", file=sys.stderr)
        else:
            print("✅ Все сообщения успешно отправлены или отброшены.", file=sys.stderr)


if __name__ == "__main__":
    main()
