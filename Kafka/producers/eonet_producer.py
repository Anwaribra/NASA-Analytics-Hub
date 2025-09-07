import asyncio
import requests
from datetime import datetime, timezone
from base_producer import BaseProducer

TOPIC = "nasa.eonet"

async def run():
    async with BaseProducer(TOPIC) as producer:
        while True:
            api_key = producer.get_api_key()
            url = f"https://eonet.gsfc.nasa.gov/api/v3/events?api_key={api_key}"
            response = requests.get(url)
            data = response.json()

            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "events": data.get("events", [])
            }

            await producer.send_message(message)
            await asyncio.sleep(300)  

if __name__ == "__main__":
    asyncio.run(run())
