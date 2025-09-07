import asyncio
import requests
from datetime import datetime, timezone
from base_producer import BaseProducer

TOPIC = "nasa.iss"

async def run():
    async with BaseProducer(TOPIC) as producer:
        while True:
            url = "http://api.open-notify.org/iss-now.json"
            response = requests.get(url)
            data = response.json()

            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "iss_position": data.get("iss_position", {}),
                "message": data.get("message", "")
            }

            await producer.send_message(message)
            await asyncio.sleep(30) 

if __name__ == "__main__":
    asyncio.run(produce())
