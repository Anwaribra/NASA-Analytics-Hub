import asyncio
import json
import os
import requests
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

NASA_API_KEY = os.getenv("API_KEY_2")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "nasa.eonet"

async def produce():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    try:
        url = f"https://eonet.gsfc.nasa.gov/api/v3/events?api_key={API_KEY_2}"
        response = requests.get(url)
        data = response.json()

        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "events": data.get("events", [])
        }

        await producer.send_and_wait(TOPIC, json.dumps(message).encode("utf-8"))
        print(f"Sent message to {TOPIC}")

    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(produce())
