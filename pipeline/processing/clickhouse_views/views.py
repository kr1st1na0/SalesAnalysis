from clickhouse_driver import Client

def create_views():
    client = Client(host='clickhouse')

    # 1. Средний чек по продавцам (за 6 месяцев) с ФИО продавца
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

    # 2. Топ-10 товаров по выручке (6 месяцев) с названием товара
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

    # 3. Дневная динамика продаж (6 месяцев)
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

    # 5. Почасовая активность продаж (за 7 дней)
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
    print("Start create...")
    create_views()
    print("Done!!!!")
