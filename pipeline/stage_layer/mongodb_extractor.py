from pymongo import MongoClient
import psycopg2

# Инициализация MongoDB
def init_mongodb():
    client = MongoClient(
        "mongodb://admin:admin@mongodb:27017/"
    )
    return client.sales

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
def create_product_table():
    command = (
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(225),
            category VARCHAR(225),
            price NUMERIC(10,2),
            cost NUMERIC(10,2),
            stock_quantity INT,
            manufacturer VARCHAR(225),
            created_at TIMESTAMP
        )
        """
    )

    conn = init_stage_layer()
    cur = conn.cursor()
    cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()

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

if __name__ == '__main__':
    print("Starting extracting data from MongoDB...")

    create_product_table()
    transfer_mongo_products()

    print("Success: data from MongoDB in stage layer")