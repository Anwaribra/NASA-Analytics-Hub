import asyncio
import json
import os
import requests
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

NASA_API_KEY = os.getenv("API_KEY_3") 
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "nasa.iss"

async def produce():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    try:
        url = "http://api.open-notify.org/iss-now.json"
        response = requests.get(url)
        data = response.json()

        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "iss_position": data.get("iss_position", {}),
            "message": data.get("message", "")
        }

        await producer.send_and_wait(TOPIC, json.dumps(message).encode("utf-8"))
        print(f"Sent message to {TOPIC}")

    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(produce())
