from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from clickhouse_driver import Client

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_transform_load():

    pg_conn = psycopg2.connect(
        host="postgres_stage",
        database="stage_layer",
        user="admin",
        password="admin"
    )
    pg_cursor = pg_conn.cursor()

    ch_client = Client(host='clickhouse')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            id UInt64,
            first_name String,
            last_name String,
            email String,
            phone String,
            registration_date Date,
            loyalty_level Int32
        ) ENGINE = MergeTree()
        ORDER BY id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS sellers (
            id UInt64,
            first_name String,
            last_name String,
            email String,
            phone String,
            hire_date Date,
            department String
        ) ENGINE = MergeTree()
        ORDER BY id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id UInt64,
            name String,
            category String,
            price Float64,
            cost Float64,
            stock_quantity Int32,
            manufacturer String,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id
    ''')

    count = ch_client.execute('SELECT count() FROM customers')[0][0]

    if count == 0:
        pg_cursor.execute("SELECT * FROM mock_data")
    else:
        yesterday = (datetime.utcnow() - timedelta(days=1)).date()
        pg_cursor.execute("""
            SELECT * FROM mock_data
            WHERE customer_registration_date = %s
               OR seller_hire_date = %s
               OR product_created_at::date = %s
        """, (yesterday, yesterday, yesterday))

    rows = pg_cursor.fetchall()

    customers = set()
    sellers = set()
    products = set()

    for row in rows:
        customers.add((
            row[1], row[2], row[3], row[4], row[5], row[6]
        ))
        sellers.add((
            row[7], row[8], row[9], row[10], row[11], row[12]
        ))
        products.add((
            row[13], row[14], float(row[15]), float(row[16]), row[17], row[18], row[19]
        ))

    customers_with_id = [(i + 1,) + row for i, row in enumerate(customers)]
    sellers_with_id = [(i + 1,) + row for i, row in enumerate(sellers)]
    products_with_id = [(i + 1,) + row for i, row in enumerate(products)]

    ch_client.execute('INSERT INTO customers VALUES', customers_with_id)
    ch_client.execute('INSERT INTO sellers VALUES', sellers_with_id)
    ch_client.execute('INSERT INTO products VALUES', products_with_id)

    pg_conn.close()


with DAG(
    dag_id='daily_batch_etl_to_clickhouse',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_transform_load
    )

    run_etl
