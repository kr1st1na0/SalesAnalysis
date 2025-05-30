import random
from datetime import datetime
from faker import Faker
from pymongo import MongoClient
import psycopg2
from kafka import KafkaProducer
import json
import time

fake = Faker()
Faker.seed(42)
random.seed(42)

def init_postgres():
    conn = psycopg2.connect(
        host="postgres",
        database="people",
        user="admin",
        password="admin"
    )
    return conn

def init_kafka():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def generate_sales(num=10000):
    producer = init_kafka()

    # Получаем ID клиентов и продавцов
    pg_conn = init_postgres()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in pg_cur.fetchall()]
    
    pg_cur.execute("SELECT seller_id FROM sellers")
    seller_ids = [row[0] for row in pg_cur.fetchall()]
    pg_conn.close()
    
    # Получаем товары
    mongo_db = MongoClient("mongodb://admin:admin@mongodb:27017/").sales
    valid_products = list(mongo_db.products.find({}, {
        '_id': 1,
        'price': 1,
        'name': 1
    }))
    
    if not valid_products:
        raise ValueError("No products found in MongoDB")
    
    for i in range(1, num + 1):
        try:
            sale_date = fake.date_time_between(start_date='-6m', end_date='now')
            customer_id = random.choice(customer_ids)
            seller_id = random.choice(seller_ids)
            product = random.choice(valid_products)
            product_id = str(product['_id'])
            quantity = random.randint(1, 5)
            
            price = product['price']
            amount = round(quantity * price, 2)
            discount = round(random.uniform(0, 0.3) if random.random() > 0.7 else 0, 2)
            
            sale_data = {
                'sale_id': f'sale_{i}',
                'customer_id': customer_id,
                'seller_id': seller_id,
                'product_id': product_id,
                'quantity': quantity,
                'sale_date': sale_date.strftime('%Y-%m-%d %H:%M:%S'),
                'amount': amount,
                'discount': discount,
                'product_name': product.get('name', 'Unknown')
            }

            producer.send('sales', value=sale_data)

            if i % 1000 == 0:
                print(f"Generated {i} sales (Product: {product['name']}, Amount: {amount})")
                time.sleep(0.1)
                
        except Exception as e:
            print(f"Error generating sale {i}: {str(e)}")
            continue
    
    producer.flush()
    print(f"Successfully generated and sent {num} sales to Kafka")

if __name__ == '__main__':
    print("Starting sales generation...")
    generate_sales(50000)
    print("Success: sales")