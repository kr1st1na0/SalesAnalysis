import psycopg2
from kafka import KafkaConsumer
import json

# Инициализация Kafka
def init_kafka():
    return KafkaConsumer(
        'sales',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        group_id='clickhouse_group',
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

# Создание таблиц в стейдж слое
def create_sales_table():
    command = (
        """
        CREATE TABLE IF NOT EXISTS sales (
            sale_id VARCHAR(50) PRIMARY KEY,
            customer_id INT REFERENCES customers(customer_id),
            customer_first_name VARCHAR(225),
            customer_last_name VARCHAR(225),
            seller_id INT REFERENCES sellers(seller_id),
            seller_first_name VARCHAR(225),
            seller_last_name VARCHAR(225),
            product_id VARCHAR(225) REFERENCES products(product_id),
            quantity INT,
            sale_date TIMESTAMP,
            amount NUMERIC(10,2),
            discount NUMERIC(10,2),
            product_name VARCHAR(225)
        )
        """
    )

    conn = init_stage_layer()
    cur = conn.cursor()
    cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()

# Берем продажи из Kafka
def consume_sales_from_kafka():
    consumer = init_kafka()

    pg_conn = init_stage_layer()
    pg_cur = pg_conn.cursor()

    for message in consumer:
        sale = message.value
        try:
            pg_cur.execute(
                """
                INSERT INTO sales (sale_id, customer_id, customer_first_name, customer_last_name, seller_id, seller_first_name, seller_last_name, product_id, quantity, sale_date, amount, discount, product_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sale_id) DO NOTHING
                """,
                (
                    sale.get('sale_id'),
                    sale.get('customer_id'),
                    sale.get('customer_first_name'),
                    sale.get('customer_last_name'),
                    sale.get('seller_id'),
                    sale.get('seller_first_name'),
                    sale.get('seller_last_name'),
                    sale.get('product_id'),
                    sale.get('quantity'),
                    sale.get('sale_date'),
                    sale.get('amount'),
                    sale.get('discount'),
                    sale.get('product_name')
                )
            )
            pg_conn.commit()
        except Exception as e:
            print(f"Error extracting sale: {e}")
            pg_conn.rollback()

    pg_cur.close()
    pg_conn.close()

if __name__ == '__main__':
    print("Starting extracting data from Kafka...")

    create_sales_table()
    consume_sales_from_kafka()

    print("Success: data from Kafka in stage layer")