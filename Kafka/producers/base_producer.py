import asyncio
import json
import os
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEYS = [
    os.getenv("API_KEY_1"),
    os.getenv("API_KEY_2"),
    os.getenv("API_KEY_3"),
]
key_index = 0

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")


class BaseProducer:
    def __init__(self, topic: str):
        self.topic = topic
        self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)

    def get_api_key(self):
        """Rotate through NASA API keys"""
        global key_index
        api_key = API_KEYS[key_index % len(API_KEYS)]
        key_index += 1
        return api_key

    async def send_message(self, message: dict):
        """Send JSON message to Kafka topic"""
        await self.producer.send_and_wait(
            self.topic,
            json.dumps(message).encode("utf-8")
        )
        print(f"[{self.topic}] Sent message")

    async def __aenter__(self):
        await self.producer.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.producer.stop()
