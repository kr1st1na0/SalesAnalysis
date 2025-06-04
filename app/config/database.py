from app.core.application import app
import asyncpg
import motor.motor_asyncio
import aiokafka
import json
from fastapi import HTTPException

async def startup_event():
    try:
        # PostgreSQL
        pg_conn = await asyncpg.connect(
            host="postgres_people",
            database="people",
            user="admin",
            password="admin"
        )

        # MongoDB
        mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
            "mongodb://admin:admin@mongodb:27017"
        )
        await mongo_client.admin.command('ping')

        # Kafka
        #! localhost заменяем на kafka
        kafka_producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version="2.5.0"
        )
        await kafka_producer.start()

        app.state.pg_conn = pg_conn
        app.state.mongo_client = mongo_client
        app.state.kafka_producer = kafka_producer

    except Exception as e:
        raise RuntimeError(f"Failed to connect to data stores: {str(e)}")

async def get_pg_connection():
    try:
        return getattr(app.state, "pg_conn")
    except AttributeError:
        raise HTTPException(status_code=500, detail="PostgreSQL connection not established")


async def get_mongo_client():
    try:
        return getattr(app.state, "mongo_client")
    except AttributeError:
        raise HTTPException(status_code=500, detail="MongoDB connection not established")


async def get_kafka_producer():
    try:
        return getattr(app.state, "kafka_producer")
    except AttributeError:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")

async def shutdown_event():
    try:
        await app.state.pg_conn.close()
        app.state.mongo_client.close()
        await app.state.kafka_producer.stop()
    except Exception as e:
        print(f"Error during shutdown: {str(e)}")