import json
import os
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv()

class BaseProducer:
    def __init__(self, topic):
        self.topic = topic
        self.producer = None
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.nasa_api_key = os.getenv("NASA_API_KEY")

    async def __aenter__(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_broker
        )
        await self.producer.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.producer.stop()

    def get_api_key(self):
        return self.nasa_api_key

    async def send_message(self, message):
        try:
            value_json = json.dumps(message).encode('utf-8')
            await self.producer.send_and_wait(self.topic, value_json)
            print(f"Message sent to topic {self.topic}")
        except Exception as e:
            print(f"Error sending message to {self.topic}: {str(e)}")
