import json
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
import time

BATCH_SIZE = 1000

def init_postgres():
    return psycopg2.connect(
        host='postgres_stage',
        database='stage_layer',
        user='admin',
        password='admin'
    )

def fetch_new_sales(last_max_id):
    conn = init_postgres()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            sale_customer_id,
            sale_seller_id,
            sale_product_id,
            sale_quantity,
            sale_date,
            sale_amount,
            sale_discount,
            id
        FROM mock_data
        WHERE id > %s
        ORDER BY id ASC
        LIMIT %s
    ''', (last_max_id, BATCH_SIZE))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def wait_for_data_ready_signal():
    consumer = KafkaConsumer(
        'control',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        group_id='control_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("[Kafka] Waiting for 'data_ready' signals in 'control' topic...")
    for message in consumer:
        event = message.value
        if event.get("event") == "data_ready":
            print("[Kafka] Received 'data_ready' signal.")
            return

def send_to_kafka(producer, rows, start_id=1):
    for idx, row in enumerate(rows, start=start_id):
        payload = {
            'sale_id': str(idx),
            'customer_id': row[0],
            'seller_id': row[1],
            'product_id': row[2],
            'quantity': row[3],
            'sale_date': row[4].strftime("%Y-%m-%d %H:%M:%S"),
            'amount': float(row[5]),
            'discount': float(row[6])
        }
        producer.send('sales_kf', value=payload)
    producer.flush()
    print(f"[Kafka] Sent {len(rows)} rows to sales_kf.")

def main():
    last_max_id = 0
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        wait_for_data_ready_signal()

        new_rows = fetch_new_sales(last_max_id)
        if not new_rows:
            print("[PostgreSQL] No new rows to process.")
            time.sleep(5)
            continue

        send_to_kafka(producer, new_rows, start_id=last_max_id+1)

        last_max_id = new_rows[-1][-1]

if __name__ == '__main__':
    main()