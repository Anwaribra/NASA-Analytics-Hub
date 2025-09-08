import asyncio
import os
import json
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from Kafka.producers.base_producer import BaseProducer

load_dotenv()

TOPIC = "nasa.eonet"


class EONETProducer(BaseProducer):
    def __init__(self):
        super().__init__(TOPIC)

    async def produce(self):
        url = f"https://eonet.gsfc.nasa.gov/api/v3/events?api_key={self.get_api_key()}"
        response = requests.get(url)
        data = response.json()
        events = data.get("events", [])

        for event in events:
            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event
            }
            await self.send_message(message)
            print(f"Sent event {event.get('id')} to {self.topic}")


async def run():
    async with EONETProducer() as producer:
        await producer.produce()


if __name__ == "__main__":
    asyncio.run(run())
