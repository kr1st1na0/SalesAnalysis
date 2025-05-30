import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2

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

def generate_phone():
    """Генерирует телефон длиной не более 15 символов"""
    return f"+{random.randint(1, 99)}{random.randint(100,999)}{random.randint(1000,9999)}"

def generate_customers(num=1000):
    conn = init_postgres()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS customers CASCADE;
        CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(50),
            registration_date DATE,
            loyalty_level INT
        );
    """)
    conn.commit()

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

if __name__ == '__main__':
    print("Starting customers generation...")
    generate_customers(1000)
    print("Success: customers")