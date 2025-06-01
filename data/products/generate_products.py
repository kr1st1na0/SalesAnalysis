import random
from datetime import datetime, timedelta
from faker import Faker
from pymongo import MongoClient

fake = Faker()
Faker.seed(42)
random.seed(42)

def init_mongodb():
    client = MongoClient(
        "mongodb://admin:admin@mongodb:27017/"
    )
    return client.sales

def generate_products(num=500):
    db = init_mongodb()
    products = db.products

    # Очищаем коллекцию перед генерацией новых данных
    products.delete_many({})
    
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Automotive', 'Books', 'Toys', 'Beauty']
    
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

if __name__ == '__main__':
    print("Starting products generation...")
    generate_products(500)
    print("Success: products")