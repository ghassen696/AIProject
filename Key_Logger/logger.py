import json
import logging
from kafka import KafkaProducer
import config
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        linger_ms=10  # small batching
    )
    logger.info("Kafka producer initialized.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    producer = None

# Local retry buffer for events if Kafka is down
local_retry_buffer = []

def log_event(event: dict):
    global producer, local_retry_buffer
    if producer is None:
        logger.error("Kafka producer is not initialized. Storing event locally.")
        local_retry_buffer.append(event)
        return

    try:
        future = producer.send(config.KAFKA_TOPIC, value=event)
        producer.flush()
        logger.debug(f"Event sent to Kafka topic {config.KAFKA_TOPIC}: {event}")
    except Exception as e:
        logger.error(f"Failed to send event to Kafka: {e}. Storing event locally.")
        local_retry_buffer.append(event)

def retry_local_buffer():
    global local_retry_buffer
    if not local_retry_buffer or producer is None:
        return

    still_failed = []
    for event in local_retry_buffer:
        try:
            producer.send(config.KAFKA_TOPIC, value=event)
            producer.flush()
            logger.info(f"Retried event sent to Kafka: {event.get('event', '')}")
        except Exception as e:
            logger.error(f"Retry failed for event: {e}")
            still_failed.append(event)

    local_retry_buffer = still_failed

# You can call retry_local_buffer() periodically or at app start to drain stored events

