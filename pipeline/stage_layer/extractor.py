from pymongo import MongoClient
import psycopg2
from kafka import KafkaConsumer
import json
from bson import ObjectId

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

# Создание таблицы в стейдж слое
def create_table():
    command = (
        """
        CREATE TABLE IF NOT EXISTS mock_data (
            id SERIAL PRIMARY KEY,
            customer_first_name VARCHAR(255),
            customer_last_name VARCHAR(255),
            customer_email VARCHAR(255),
            customer_phone VARCHAR(255),
            customer_registration_date DATE,
            customer_loyalty_level INT,

            seller_first_name VARCHAR(255),
            seller_last_name VARCHAR(255),
            seller_email VARCHAR(255),
            seller_phone VARCHAR(255),
            seller_hire_date DATE,
            seller_department VARCHAR(255),

            product_name VARCHAR(225),
            product_category VARCHAR(225),
            product_price NUMERIC(10,2),
            product_cost NUMERIC(10,2),
            product_stock_quantity INT,
            product_manufacturer VARCHAR(225),
            product_created_at TIMESTAMP,

            sale_customer_id INT,
            sale_seller_id INT,
            sale_product_id VARCHAR(255),
            sale_quantity INT,
            sale_date TIMESTAMP,
            sale_amount NUMERIC(10,2),
            sale_discount NUMERIC(10,2)
        )
        """
    )

    conn = init_stage_layer()
    cur = conn.cursor()
    cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()

# Берем данные из PostgreSQL
def get_customer(customer_id):
    conn = init_postgres()
    cur = conn.cursor()

    cur.execute("""
        SELECT first_name, last_name, email, phone, registration_date, loyalty_level
        FROM customers WHERE customer_id = %s
    """, (customer_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    if result:
        return {
            "first_name": result[0],
            "last_name": result[1],
            "email": result[2],
            "phone": result[3],
            "registration_date": result[4],
            "loyalty_level": result[5]
        }
    else:
        return {k: None for k in ["first_name", "last_name", "email", "phone", "registration_date", "loyalty_level"]}


def get_seller(seller_id):
    conn = init_postgres()
    cur = conn.cursor()

    cur.execute("""
        SELECT first_name, last_name, email, phone, hire_date, department
        FROM sellers WHERE seller_id = %s
    """, (seller_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    if result:
        return {
            "first_name": result[0],
            "last_name": result[1],
            "email": result[2],
            "phone": result[3],
            "hire_date": result[4],
            "department": result[5]
        }
    else:
        return {k: None for k in ["first_name", "last_name", "email", "phone", "hire_date", "department"]}

# Берем данные из MongoDB
def get_product(product_id):
    db = init_mongodb()
    try:
        if isinstance(product_id, str):
            product_id = ObjectId(product_id)
        product = db.products.find_one({"_id": product_id})
    except Exception as e:
        print(f"Error converting product_id {product_id}: {e}")
        return {k: None for k in [
            "name", "category", "price", "cost", "stock_quantity", "manufacturer", "created_at"
        ]}

    if product:
        return {
            "name": product.get("name"),
            "category": product.get("category"),
            "price": product.get("price"),
            "cost": product.get("cost"),
            "stock_quantity": product.get("stock_quantity"),
            "manufacturer": product.get("manufacturer"),
            "created_at": product.get("created_at")
        }
    else:
        return {k: None for k in [
            "name", "category", "price", "cost", "stock_quantity", "manufacturer", "created_at"
        ]}


# Берем данные из Kafka и запись всего стейдж слой
def consume_and_merge_sales():
    consumer = init_kafka()
    pg_conn = init_stage_layer()
    pg_cur = pg_conn.cursor()

    for message in consumer:
        sale = message.value

        try:
            # Извлекаем данные из Kafka
            sale_customer_id = sale.get('customer_id')
            sale_seller_id = sale.get('seller_id')
            sale_product_id = sale.get('product_id')
            sale_quantity = sale.get('quantity')
            sale_date = sale.get('sale_date')
            sale_amount = sale.get('amount')
            sale_discount = sale.get('discount', 0)

            # Получаем данные из других источников
            customer = get_customer(sale_customer_id)
            seller = get_seller(sale_seller_id)
            product = get_product(sale_product_id)

            # Вставляем в таблицу стейдж слоя
            pg_cur.execute("""
                INSERT INTO mock_data (
                    customer_first_name, customer_last_name, customer_email, customer_phone,
                    customer_registration_date, customer_loyalty_level,
                    
                    seller_first_name, seller_last_name, seller_email, seller_phone,
                    seller_hire_date, seller_department,
                    
                    product_name, product_category, product_price, product_cost,
                    product_stock_quantity, product_manufacturer, product_created_at,
                    
                    sale_customer_id, sale_seller_id, sale_product_id,
                    sale_quantity, sale_date, sale_amount, sale_discount
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                customer["first_name"], customer["last_name"], customer["email"], customer["phone"],
                customer["registration_date"], customer["loyalty_level"],
                
                seller["first_name"], seller["last_name"], seller["email"], seller["phone"],
                seller["hire_date"], seller["department"],
                
                product["name"], product["category"], product["price"], product["cost"],
                product["stock_quantity"], product["manufacturer"], product["created_at"],
                
                sale_customer_id, sale_seller_id, sale_product_id,
                sale_quantity, sale_date, sale_amount, sale_discount
            ))

            pg_conn.commit()

        except Exception as e:
            print(f"Error processing sale: {e}")
            pg_conn.rollback()

    pg_cur.close()
    pg_conn.close()


if __name__ == '__main__':
    print("Starting extracting data...")

    create_table()

    consume_and_merge_sales()

    print("Success: data in stage layer")