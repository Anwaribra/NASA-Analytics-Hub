import asyncio
import os
import requests
import math
from datetime import datetime, timezone
import sys
from typing import Dict, Any
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from Kafka.producers.base_producer import BaseProducer

TOPIC = "nasa.iss"
ISS_MEAN_ALTITUDE = 408000  
EARTH_RADIUS = 6371000  # Earth's gravitational parameter (m³/s²)

def calculate_orbital_elements(lat: float, lon: float, altitude: float = ISS_MEAN_ALTITUDE) -> Dict[str, Any]:
    """Calculate Keplerian orbital elements from lat/lon/altitude."""
   
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    
   
    r = EARTH_RADIUS + altitude
    x = r * math.cos(lat_rad) * math.cos(lon_rad)
    y = r * math.cos(lat_rad) * math.sin(lon_rad)
    z = r * math.sin(lat_rad)
    
    
    v = math.sqrt(EARTH_MU / r)
    
    period = 2 * math.pi * math.sqrt(r**3 / EARTH_MU)
    
    ecc = 0.0009
    inc = 51.6
    return {
        "semi_major_axis": r,  
        "eccentricity": ecc,
        "inclination": inc,  # degrees
        "velocity": v,  
        "period": period,  
        "position_vector": {
            "x": x,
            "y": y,
            "z": z
        }
    }

async def run():
    async with BaseProducer(TOPIC) as producer:
        while True:
            url = "http://api.open-notify.org/iss-now.json"
            response = requests.get(url)
            data = response.json()
            
            iss_position = data.get("iss_position", {})
            lat = float(iss_position.get("latitude", 0))
            lon = float(iss_position.get("longitude", 0))
 
            orbital_elements = calculate_orbital_elements(lat, lon)
            
            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "iss_position": iss_position,
                "message": data.get("message", ""),
                "orbital_elements": orbital_elements
            }

            await producer.send_message(message)
            await asyncio.sleep(30) 

if __name__ == "__main__":
    asyncio.run(run())
