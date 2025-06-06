import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2

fake = Faker()
Faker.seed(42)
random.seed(42)

def init_postgres():
    conn = psycopg2.connect(
        host="postgres_people",
        database="people",
        user="admin",
        password="admin"
    )
    return conn

def generate_phone():
    """Генерирует телефон длиной не более 15 символов"""
    return f"+{random.randint(1, 99)}{random.randint(100,999)}{random.randint(1000,9999)}"

def generate_sellers(num=50):
    conn = init_postgres()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS sellers CASCADE;
        CREATE TABLE sellers (
            seller_id SERIAL PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(255),
            hire_date DATE,
            department VARCHAR(255)
        );
    """)
    conn.commit()
    
    departments = ['Electronics', 'Clothing', 'Home', 'Sports', 'Automotive', 'Books', 'Toys', 'Beauty']
    
    for _ in range(num):
        phone = generate_phone()
        
        cur.execute(
            "INSERT INTO sellers (first_name, last_name, email, phone, hire_date, department) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                phone,
                datetime.now(),
                random.choice(departments)
            )
        )
    
    conn.commit()
    conn.close()
    print(f"Generated {num} sellers")

if __name__ == '__main__':
    print("Starting sellers generation...")
    generate_sellers(50)
    print("Success: sellers")