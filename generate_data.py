import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from pymongo import MongoClient
from kafka import KafkaProducer
import json
import time
from clickhouse_driver import Client as ClickHouseClient

# Персборка докер файла docker-compose build --no-cache

'''
Полная пересборка с удалением всех образов:

docker-compose down --volumes --remove-orphans \
  && docker-compose build --no-cache \
  && docker-compose up --force-recreate


проверка mongo
docker exec -it project_python-mongodb-1 mongosh
use sales
show collections
db.products.find({}, { name: 1, price: 1 }).limit(5).pretty()

постгря
docker exec -it project_python-postgres-1 psql -U admin -d sales

кликхаус
docker exec -it clickhouse clickhouse-client
SELECT * FROM sales_stage LIMIT 5;
'''



# Инициализация
fake = Faker()
Faker.seed(42)
random.seed(42)

# Подключения
def init_postgres():
    conn = psycopg2.connect(
        host="postgres",
        database="sales",
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
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def init_clickhouse():
    try:
        client = ClickHouseClient(host='clickhouse')
        client.execute('SELECT 1')
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        raise

def generate_phone():
    """Генерирует телефон длиной не более 15 символов"""
    return f"+{random.randint(1, 99)}{random.randint(100,999)}{random.randint(1000,9999)}"

def generate_customers(num=1000):
    conn = init_postgres()
    cur = conn.cursor()
    
    # Генерация данных
    for _ in range(num):
        phone = generate_phone()
        
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email, phone, registration_date, loyalty_level) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                phone,
                fake.date_between(start_date='-2y', end_date='today'),
                random.randint(1, 5)
            )
        )
    
    conn.commit()
    conn.close()
    print(f"Generated {num} customers")

def generate_sellers(num=50):
    conn = init_postgres()
    cur = conn.cursor()
    
    
    departments = ['Electronics', 'Clothing', 'Home', 'Sports', 'Automotive']
    
    for _ in range(num):
        phone = generate_phone()
        
        cur.execute(
            "INSERT INTO sellers (first_name, last_name, email, phone, hire_date, department) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                phone,
                fake.date_between(start_date='-5y', end_date='today'),
                random.choice(departments)
            )
        )
    
    conn.commit()
    conn.close()
    print(f"Generated {num} sellers")

def generate_products(num=500):
    db = init_mongodb()
    products = db.products
    
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Automotive', 'Books', 'Toys']
    
    for _ in range(num):
        product = {
            "name": fake.catch_phrase(),
            "category": random.choice(categories),
            "price": round(random.uniform(10, 1000), 2),
            "cost": round(random.uniform(5, 800), 2),
            "stock_quantity": random.randint(0, 500),
            "manufacturer": fake.company(),
            "created_at": datetime.now()
        }
        products.insert_one(product)
    
    print(f"Generated {num} products")
    count = products.count_documents({"price": {"$exists": True}})
    print(f"Products with price: {count}/{num}")

def generate_sales(num=10000):
    producer = init_kafka()
    try:
        ch = init_clickhouse()
        
        # Создаем таблицу в ClickHouse
        ch.execute('''
        CREATE TABLE IF NOT EXISTS sales_stage (
            sale_id String,
            customer_id Int32,
            seller_id Int32,
            product_id String,
            quantity Int32,
            sale_date DateTime,
            amount Float64,
            discount Float64
        ) ENGINE = MergeTree()
        ORDER BY sale_date
        ''')
    except Exception as e:
        print(f"Error setting up ClickHouse: {e}")
        return
    
    # Получаем ID клиентов и продавцов
    pg_conn = init_postgres()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in pg_cur.fetchall()]
    
    pg_cur.execute("SELECT seller_id FROM sellers")
    seller_ids = [row[0] for row in pg_cur.fetchall()]
    pg_conn.close()
    
    # Получаем товары
    mongo_db = init_mongodb()
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
                'sale_date': sale_date,
                'amount': amount,
                'discount': discount,
                'product_name': product.get('name', 'Unknown')
            }
            
            kafka_data = sale_data.copy()
            kafka_data['sale_date'] = sale_date.strftime('%Y-%m-%d %H:%M:%S')

            producer.send('sales', value=kafka_data)

            # Записываем в ClickHouse
            ch.execute(
                'INSERT INTO sales_stage VALUES',
                [{
                    'sale_id': sale_data['sale_id'],
                    'customer_id': sale_data['customer_id'],
                    'seller_id': sale_data['seller_id'],
                    'product_id': sale_data['product_id'],
                    'quantity': sale_data['quantity'],
                    'sale_date': sale_data['sale_date'],
                    'amount': sale_data['amount'],
                    'discount': sale_data['discount']
                }]
            )
            
            if i % 1000 == 0:
                print(f"Generated {i} sales (Product: {product['name']}, Amount: {amount})")
                time.sleep(0.1)
                
        except Exception as e:
            print(f"Error generating sale {i}: {str(e)}")
            continue
    
    producer.flush()
    print(f"Successfully generated {num} sales")

if __name__ == '__main__':
    print("Starting data generation...")
    
    generate_customers(1000)
    generate_sellers(50)
    generate_products(500)
    generate_sales(50000)
    
    print("Data generation completed!")