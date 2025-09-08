import asyncio
import os
import requests
from datetime import datetime, timezone
import sys


# this to allows importing from the kafka package
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from Kafka.producers.base_producer import BaseProducer


TOPIC = "nasa.neo"


async def run():
    async with BaseProducer(TOPIC) as producer:
        while True:
            api_key = producer.get_api_key()
            url = f"https://api.nasa.gov/neo/rest/v1/feed?api_key={api_key}"
            response = requests.get(url)
            data = response.json()

            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "neos": data.get("near_earth_objects", {})
            }

            await producer.send_message(message)
            await asyncio.sleep(60)     

if __name__ == "__main__":
    asyncio.run(run())
