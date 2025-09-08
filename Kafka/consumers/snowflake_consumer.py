import os
import json
import asyncio
from aiokafka import AIOKafkaConsumer
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPICS = ["nasa.neo", "nasa.eonet", "nasa.iss"]

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

def insert_into_snowflake(topic, message):
    """insert message into snowflake table based on topic"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()

    try:
        if topic == "nasa.neo":
            sql = """
                INSERT INTO NEO_EVENTS (EVENT_TIME, DATA)
                SELECT %s, PARSE_JSON(%s)
            """
            cur.execute(sql, (message["timestamp"], json.dumps(message["neos"])))

        elif topic == "nasa.eonet":
            sql = """
                INSERT INTO EONET_EVENTS (EVENT_TIME, DATA)
                SELECT %s, PARSE_JSON(%s)
            """
            cur.execute(sql, (message["timestamp"], json.dumps(message["event"])))

        elif topic == "nasa.iss":
            sql = """
                INSERT INTO ISS_EVENTS (EVENT_TIME, POSITION_DATA, MESSAGE)
                SELECT %s, PARSE_JSON(%s), %s
            """
            cur.execute(sql, (
                message["timestamp"], 
                json.dumps(message["iss_position"]),
                message["message"]
            ))

        conn.commit()
        print(f"inserted message into {topic} -> Snowflake ")

    except Exception as e:
        print(f"error inserting into Snowflake: {e}")
    finally:
        cur.close()
        conn.close()


async def consume():
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="nasa-consumers"
    )

    await consumer.start()
    try:
        async for msg in consumer:
            try:
                message = json.loads(msg.value.decode("utf-8"))
                insert_into_snowflake(msg.topic, message)
            except Exception as e:
                print(f"failed to process message: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
