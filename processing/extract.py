from pymongo import MongoClient
import psycopg2
from kafka import KafkaConsumer
import json

# Инициализация всех бдшек
def init_postgres():
    conn = psycopg2.connect(
        host="postgres",
        database="people",
        user="admin",
        password="admin"
    )
    return conn

def init_mongodb():
    client = MongoClient(
        "mongodb://admin:admin@mongodb:27017/"
    )
    return client.sales

def init_kafka():
    return KafkaConsumer(
        'sales',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

# Инициализация стейдж слоя
def init_stage_layer():
    conn = psycopg2.connect(
        host="postgres_stage",
        database="stage_layer",
        user="admin",
        password="admin"
    )
    return conn

'''

    '''