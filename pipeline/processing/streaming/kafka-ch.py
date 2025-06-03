import json
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from clickhouse_driver import Client as ClickHouseClient
from datetime import datetime

# PostgreSQL -> Kafka
def fetch_sales_from_postgres():
    try:
        conn = psycopg2.connect(
            host='postgres_stage',
            database='stage_layer',
            user='admin',
            password='admin'
        )
        cursor = conn.cursor()
        cursor.execute('''
            SELECT
                sale_customer_id,
                sale_seller_id,
                sale_product_id,
                sale_quantity,
                sale_date,
                sale_amount,
                sale_discount
            FROM mock_data
            WHERE sale_customer_id IS NOT NULL
              AND sale_seller_id IS NOT NULL
              AND sale_product_id IS NOT NULL
              AND sale_date IS NOT NULL
        ''')
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"[PostgreSQL] Error: {e}")
        return []

def send_to_kafka(rows):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    sale_id_counter = 1
    for row in rows:
        payload = {
            'sale_id': str(sale_id_counter),
            'customer_id': row[0],
            'seller_id': row[1],
            'product_id': row[2],
            'quantity': row[3],
            'sale_date': row[4].strftime("%Y-%m-%d %H:%M:%S"),
            'amount': float(row[5]),
            'discount': float(row[6])
        }
        producer.send('sales_kf', value=payload)
        sale_id_counter += 1
    producer.flush()
    print(f"[Kafka] Sent {len(rows)} rows.")

# Kafka -> ClickHouse
def init_clickhouse():
    try:
        client = ClickHouseClient(host='clickhouse')
        client.execute('SELECT 1')
        return client
    except Exception as e:
        print(f"[ClickHouse] Connection error: {e}")
        raise

def create_clickhouse_table(client):
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
        try:
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
                print(f"[ClickHouse] Inserted {len(batch)} rows")
                batch.clear()
        except Exception as e:
            print(f"[Kafka->ClickHouse] Error: {e}")

    if batch:
        ch.execute('INSERT INTO sales_facts VALUES', batch)
        print(f"[ClickHouse] Inserted final batch of {len(batch)} rows")

if __name__ == '__main__':
    print("Starting PostgreSQL to Kafka...")
    sales_data = fetch_sales_from_postgres()
    if sales_data:
        send_to_kafka(sales_data)

        print(" Starting Kafka load data into ClickHouse...")
        consume_and_insert()
    else:
        print("No sales data found in PostgreSQL.")
