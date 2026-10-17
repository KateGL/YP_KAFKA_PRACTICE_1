import logging
import uuid
import time
from confluent_kafka import Producer, KafkaException
from ..message import Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "kafka-1:29092"
TOPIC = "test-topic"

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}"
        )

def main():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 100,
        "max.in.flight.requests.per.connection": 5,
    }
    producer = Producer(conf)

    while True:
        msg = Message(
            id=str(uuid.uuid4()),
            payload={"value": "demo-payload", "ts": time.time()},
            metadata={"source": "python-producer"}
        )
        serialized = msg.serialize()
        logger.info(f"Sending message: {msg}")

        try:
            producer.produce(
                topic=TOPIC,
                value=serialized,
                callback=delivery_report
            )
            # Важно: вызываем poll, чтобы доставить callback и не переполнить буфер
            producer.poll(0.1)
        except BufferError:
            logger.warning("Producer buffer full, waiting...")
            producer.flush(10)
        except KafkaException as e:
            logger.error(f"Producer error: {e}", exc_info=True)

        time.sleep(1)

    producer.flush()

if __name__ == "__main__":
    main()
