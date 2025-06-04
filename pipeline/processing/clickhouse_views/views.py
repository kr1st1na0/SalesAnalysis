import time
from clickhouse_driver import Client

REQUIRED_TABLES = {
    "customers": "SELECT count() FROM customers",
    "sellers": "SELECT count() FROM sellers",
    "products": "SELECT count() FROM products",
    "sales_facts": "SELECT count() FROM sales_facts",
}

def wait_for_required_tables(client, timeout=600):
    print("[ClickHouse] Ожидание загрузки всех необходимых таблиц...")
    start_time = time.time()
    ready = {table: False for table in REQUIRED_TABLES}

    while True:
        for table, query in REQUIRED_TABLES.items():
            if ready[table]:
                continue
            try:
                result = client.execute(query)
                count = result[0][0]
                if count > 0:
                    ready[table] = True
                    print(f"[ClickHouse] Таблица '{table}' готова ({count} строк).")
                else:
                    print(f"[ClickHouse] Таблица '{table}' существует, но пуста.")
            except Exception as e:
                if "doesn't exist" in str(e):
                    print(f"[ClickHouse] Таблица '{table}' ещё не создана.")
                else:
                    print(f"[ClickHouse] Ошибка при проверке таблицы '{table}': {e}")

        if all(ready.values()):
            print("[ClickHouse] Все таблицы готовы. Продолжаем.")
            break

        if time.time() - start_time > timeout:
            raise TimeoutError("Превышено время ожидания загрузки всех таблиц.")

        time.sleep(30)

def create_views():
    client = Client(host='clickhouse')
    wait_for_required_tables(client)

    # 1. Средний чек по продавцам (за 6 месяцев)
    client.execute('''
    CREATE MATERIALIZED VIEW IF NOT EXISTS avg_check_by_seller
    ENGINE = AggregatingMergeTree
    ORDER BY seller_full_name
    POPULATE AS
    SELECT
        concat(s.first_name, ' ', s.last_name) AS seller_full_name,
        count() AS sale_count,
        avgState(sale_amount - sale_discount) AS avg_check_state
    FROM sales_facts sf
    ANY INNER JOIN sellers s ON sf.seller_id = s.id
    WHERE sale_date >= now() - INTERVAL 6 MONTH
    GROUP BY seller_full_name
    ''')

    # 2. Топ-10 товаров по выручке
    client.execute('''
    CREATE MATERIALIZED VIEW IF NOT EXISTS top_products
    ENGINE = AggregatingMergeTree
    ORDER BY product_name
    POPULATE AS
    SELECT
        p.name AS product_name,
        sumState(sale_amount - sale_discount) AS revenue_state
    FROM sales_facts sf
    ANY INNER JOIN products p ON sf.product_id = p.id
    WHERE sale_date >= now() - INTERVAL 6 MONTH
    GROUP BY product_name
    ''')

    # 3. Дневная динамика продаж
    client.execute('''
    CREATE MATERIALIZED VIEW IF NOT EXISTS daily_sales_summary
    ENGINE = SummingMergeTree
    ORDER BY sale_day
    POPULATE AS
    SELECT
        toDate(sale_date) AS sale_day,
        count() AS total_sales,
        sum(sale_amount - sale_discount) AS total_revenue
    FROM sales_facts
    WHERE sale_date >= now() - INTERVAL 6 MONTH
    GROUP BY sale_day
    ''')

    # 4. Продажи по категориям товаров
    client.execute('''
    CREATE MATERIALIZED VIEW IF NOT EXISTS revenue_by_category
    ENGINE = AggregatingMergeTree
    ORDER BY product_category
    POPULATE AS
    SELECT
        p.category AS product_category,
        sumState(sale_amount - sale_discount) AS total_revenue_state
    FROM sales_facts sf
    ANY INNER JOIN products p ON sf.product_id = p.id
    WHERE sale_date >= now() - INTERVAL 6 MONTH
    GROUP BY product_category
    ''')

    # 5. Почасовая активность продаж (7 дней)
    client.execute('''
    CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_sales_activity
    ENGINE = SummingMergeTree
    ORDER BY sale_hour
    POPULATE AS
    SELECT
        toStartOfHour(sale_date) AS sale_hour,
        count() AS sale_count,
        sum(sale_amount - sale_discount) AS revenue
    FROM sales_facts
    WHERE sale_date >= now() - INTERVAL 7 DAY
    GROUP BY sale_hour
    ''')

if __name__ == '__main__':
    print("[ClickHouse] Запуск создания витрин...")
    create_views()
    print("[ClickHouse] Все витрины созданы.")
