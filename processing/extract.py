from pymongo import MongoClient
import psycopg2
from kafka import KafkaConsumer
import json

# Инициализация сорсов
def init_postgres():
    conn = psycopg2.connect(
        host="postgres_people",
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

# Создание таблиц в стейдж слое
def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(50),
            registration_date DATE,
            loyalty_level INT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sellers (
            seller_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(50),
            hire_date DATE,
            department VARCHAR(50)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(50),
            category VARCHAR(50),
            price NUMERIC(10,2),
            cost NUMERIC(10,2),
            stock_quantity INT,
            manufacturer VARCHAR(50),
            created_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sales (
            sale_id VARCHAR(50) PRIMARY KEY,
            customer_id INT REFERENCES customers(customer_id),
            customer_first_name VARCHAR(50),
            customer_last_name VARCHAR(50),
            seller_id INT REFERENCES sellers(seller_id),
            seller_first_name VARCHAR(50),
            seller_last_name VARCHAR(50),
            product_id VARCHAR(50) REFERENCES products(product_id),
            quantity INT,
            sale_date TIMESTAMP,
            amount NUMERIC(10,2),
            discount NUMERIC(10,2),
            product_name VARCHAR(50)
        )
        """
    )

    conn = init_stage_layer()
    cur = conn.cursor()
    for command in commands:
        cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()

# Берем клиентов и продавцов из PostgreSQL
def transfer_postgres_data():
    src_conn = init_postgres()
    src_cur = src_conn.cursor()

    dest_conn = init_stage_layer()
    dest_cur = dest_conn.cursor()

    # Копируем sellers
    src_cur.execute("SELECT * FROM sellers")
    dest_cur.executemany("INSERT INTO sellers VALUES (%s, %s, %s)", src_cur.fetchall())

    # Копируем customers
    src_cur.execute("SELECT * FROM customers")
    dest_cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", dest_cur.fetchall())

    dest_conn.commit()
    dest_cur.close()
    dest_conn.close()
    src_cur.close()
    src_conn.close()
    print("Sellers and customers data has been extracted")


# Берем товары из MongoDB
def transfer_mongo_products():
    db = init_mongodb()
    mongo_collections = db.products

    pg_conn = init_stage_layer()
    pg_cur = pg_conn.cursor()

    count = 0
    for product in mongo_collections.find():
        try:
            pg_cur.execute(
                """
                INSERT INTO products (product_id, name, category, price, cost, stock_quantity, manufacturer, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
                """,
                (
                    str(product.get('_id')),
                    product.get('name'),
                    product.get('category'),
                    product.get('price'),
                    product.get('cost'),
                    product.get('stock_quantity'),
                    product.get('manufacturer'),
                    product.get('created_at')
                )
            )
            count += 1
        except Exception as e:
            print(f"Error extracting product: {e}")

    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    print(f"{count} products have been extracted")


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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    print("Starting extracting data...")

    create_tables()

    transfer_postgres_data()

    transfer_mongo_products()

    consume_sales_from_kafka()

    print("Success: data in stage layer")