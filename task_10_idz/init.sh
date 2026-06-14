#!/bin/bash
set -e

echo "=== Инициализация базы данных ==="

# Создаём пользователя airflow и БД, как раньше
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
            CREATE USER airflow WITH PASSWORD 'airflow';
        END IF;
    END
    \$\$;

    SELECT 'CREATE DATABASE airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

# Переключаемся в базу airflow и создаём схему delivery
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "airflow" <<-EOSQL
    -- Создаём схему для наших таблиц
    CREATE SCHEMA IF NOT EXISTS delivery;
    ALTER SCHEMA delivery OWNER TO airflow;
    GRANT ALL ON SCHEMA delivery TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA delivery GRANT ALL ON TABLES TO airflow;

    -- Теперь создаём таблицы внутри схемы delivery
    CREATE TABLE IF NOT EXISTS delivery.restaurants (
        restaurant_id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        cuisine VARCHAR(100),
        rating DECIMAL(3,2),
        address VARCHAR(500),
        latitude DECIMAL(10,8),
        longitude DECIMAL(11,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS delivery.dishes (
        dish_id SERIAL PRIMARY KEY,
        restaurant_id INTEGER REFERENCES delivery.restaurants(restaurant_id),
        name VARCHAR(200) NOT NULL,
        category VARCHAR(100),
        current_price DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS delivery.customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(200),
        phone VARCHAR(20),
        address VARCHAR(500),
        latitude DECIMAL(10,8),
        longitude DECIMAL(11,8),
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS delivery.orders (
        order_id SERIAL PRIMARY KEY,
        restaurant_id INTEGER REFERENCES delivery.restaurants(restaurant_id),
        customer_id INTEGER REFERENCES delivery.customers(customer_id),
        order_time TIMESTAMP NOT NULL,
        delivery_time TIMESTAMP,
        status VARCHAR(50) DEFAULT 'pending',
        total_amount DECIMAL(10,2),
        items JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS delivery.reviews (
        review_id SERIAL PRIMARY KEY,
        dish_id INTEGER REFERENCES delivery.dishes(dish_id),
        customer_id INTEGER REFERENCES delivery.customers(customer_id),
        rating INTEGER CHECK (rating >= 1 AND rating <= 5),
        comment TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    SELECT 'Таблицы созданы в схеме delivery' AS status;
EOSQL

echo "=== Инициализация завершена ==="