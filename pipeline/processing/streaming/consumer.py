import json
from kafka import KafkaConsumer
from clickhouse_driver import Client as ClickHouseClient
from datetime import datetime

BATCH_SIZE = 100


def init_clickhouse():
    client = ClickHouseClient(host='clickhouse')
    client.execute('SELECT 1')
    return client


def create_clickhouse_table(client):
    client.execute('''
        CREATE TABLE IF NOT EXISTS sales_facts (
            sale_id UInt64,
            customer_id UInt64,
            seller_id UInt64,
            product_id UInt64,
            sale_quantity Int32,
            sale_date DateTime,
            sale_amount Float64,
            sale_discount Float64
        ) ENGINE = MergeTree()
        ORDER BY sale_id
    ''')


def consume_and_insert():
    consumer = KafkaConsumer(
        'sales_kf',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        group_id='clickhouse_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    ch = init_clickhouse()
    create_clickhouse_table(ch)

    batch = []
    for message in consumer:
        data = message.value
        try:
            batch.append((
                int(data['sale_id']),
                int(data['customer_id']),
                int(data['seller_id']),
                int(data['product_id']),
                int(data['quantity']),
                datetime.strptime(data['sale_date'], "%Y-%m-%d %H:%M:%S"),
                float(data['amount']),
                float(data['discount'])
            ))

            if len(batch) >= BATCH_SIZE:
                ch.execute('INSERT INTO sales_facts VALUES', batch)
                print(f"[Consumer] Inserted batch of {len(batch)} rows")
                batch.clear()
        except Exception as e:
            print(f"[Consumer] Error: {e}")

    if batch:
        ch.execute('INSERT INTO sales_facts VALUES', batch)
        print(f"[Consumer] Inserted final batch of {len(batch)} rows")


if __name__ == '__main__':
    consume_and_insert()