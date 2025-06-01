import json
from clickhouse_driver import Client as ClickHouseClient
from kafka import KafkaConsumer
from datetime import datetime

def init_clickhouse():
    try:
        client = ClickHouseClient(host='clickhouse')
        client.execute('SELECT 1')
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        raise

def create_table(client):
    client.execute('''
    CREATE TABLE IF NOT EXISTS sales_facts (
        sale_id String,
        customer_id UInt64,
        seller_id UInt64,
        product_id UInt64,
        sale_quantity Int32,
        sale_date DateTime,
        sale_amount Float64,
        sale_discount Float64
    ) ENGINE = MergeTree()
    ORDER BY sale_date
    ''')

def consume_from_kafka():
    consumer = KafkaConsumer(
        'sales',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        group_id='clickhouse_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    ch = init_clickhouse()
    create_table(ch)

    batch = []
    for message in consumer:
        data = message.value
        batch.append((
            data['sale_id'],
            data['customer_id'],
            data['seller_id'],
            data['product_id'],
            data['quantity'],
            datetime.strptime(data['sale_date'], "%Y-%m-%d %H:%M:%S"),
            data['amount'],
            data['discount']
        ))

        if len(batch) >= 1000:
            ch.execute('INSERT INTO sales_facts VALUES', batch)
            print(f"Inserted {len(batch)} records into ClickHouse")
            batch.clear()

    if batch:
        ch.execute('INSERT INTO sales_facts VALUES', batch)
        print(f"Inserted final batch of {len(batch)} records into ClickHouse")

if __name__ == '__main__':
    print("Starting Kafka to ClickHouse consumer...")
    consume_from_kafka()