import psycopg2

# Инициализация PostgreSQL
def init_postgres():
    conn = psycopg2.connect(
        host="postgres_people",
        database="people",
        user="admin",
        password="admin"
    )
    return conn

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
def create_cus_sel_tables():
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
    sellers_data = src_cur.fetchall()
    dest_cur.executemany(
        "INSERT INTO sellers (seller_id, first_name, last_name, email, phone, hire_date, department) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        sellers_data
    )

    # Копируем customers
    src_cur.execute("SELECT * FROM customers")
    customers_data = src_cur.fetchall()
    dest_cur.executemany(
        "INSERT INTO customers (customer_id, first_name, last_name, email, phone, registration_date, loyalty_level) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        customers_data
    )

    dest_conn.commit()
    dest_cur.close()
    dest_conn.close()
    src_cur.close()
    src_conn.close()
    print("Sellers and customers data has been extracted")

if __name__ == '__main__':
    print("Starting extracting data from PostgresSQL...")

    create_cus_sel_tables()
    transfer_postgres_data()

    print("Success: data from PostgresSQL in stage layer")