-- Инициализация таблиц
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

DROP TABLE IF EXISTS sellers CASCADE;
CREATE TABLE sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(50),
    hire_date DATE,
    department VARCHAR(50)
);

