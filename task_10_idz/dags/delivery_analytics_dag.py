import os
import json
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from faker import Faker
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from folium.plugins import HeatMap

fake = Faker('ru_RU')

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'delivery_analytics_pipeline',
    default_args=default_args,
    description='Витрина для анализа доставки обедов',
    schedule_interval='@daily',
    catchup=False
)


def generate_data(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Очистка существующих данных в схеме delivery
    cur.execute("TRUNCATE delivery.restaurants, delivery.dishes, delivery.customers, delivery.orders, delivery.reviews RESTART IDENTITY CASCADE")

    # Словарь блюд по кухням
    menu_by_cuisine = {
        'Итальянская': ['Пицца Маргарита', 'Пицца Пепперони', 'Паста Карбонара', 'Лазанья', 'Ризотто с грибами', 'Тирамису', 'Брускетта', 'Кальцоне', 'Равиоли с сыром', 'Джелато'],
        'Японская': ['Суши Филадельфия', 'Ролл Калифорния', 'Сашими лосось', 'Удон с креветками', 'Рамен тонный', 'Темпура', 'Гедза', 'Окономияки', 'Чираши', 'Мисо суп'],
        'Китайская': ['Утка по-пекински', 'Курица гунбао', 'Свинина в кисло-сладком', 'Жареный рис по-янчжоу', 'Вонтоны', 'Харумаки', 'Говядина с брокколи', 'Лапша чоу мейн', 'Мандариновая рыба', 'Пельмени цзяоцзы'],
        'Русская': ['Борщ', 'Щи', 'Пельмени сибирские', 'Бефстроганов', 'Солянка', 'Оливье', 'Сельдь под шубой', 'Блины с икрой', 'Котлеты по-киевски', 'Вареники с вишней'],
        'Грузинская': ['Хачапури по-аджарски', 'Хинкали', 'Цыплёнок табака', 'Сациви', 'Лобио', 'Пхали', 'Чашушули', 'Чахохбили', 'Мцвади', 'Купаты'],
        'Французская': ['Круассан', 'Суп вшисуаз', 'Гратен дофинуа', 'Конфи из утки', 'Крем-брюле', 'Киш лорен', 'Макаруны', 'Фуа-гра', 'Буйабес', 'Ратлуй'],
        'Мексиканская': ['Тако', 'Буррито', 'Кесадилья', 'Начос', 'Гуакамоле', 'Пико де галло', 'Чили кон карне', 'Энчилада', 'Тостада', 'Сальса верде'],
        'Тайская': ['Том ям', 'Тайский зелёный карри', 'Пад тай', 'Сом там', 'Кунг тод', 'Кхао сой', 'Ям ныа', 'Паненг', 'Тайский рис с ананасом', 'Кхао ниау мамуанг'],
        'Индийская': ['Курица тандури', 'Баттер чикен', 'Наан', 'Тикка масала', 'Самоса', 'Бирани', 'Дал махани', 'Панир тикка', 'Райта', 'Гулаб джамун'],
        'Американская': ['Бургер с сыром', 'Жареные крылья', 'Картофель фри', 'Хот-дог', 'Чизкейк', 'Брауни', 'Мак энд чиз', 'Бекон', 'Панкейки с кленовым сиропом', 'Грибы портобелло']
    }
    categories = ['Салаты', 'Супы', 'Горячее', 'Десерты', 'Напитки', 'Закуски']

    # 1. Рестораны
    cuisines = list(menu_by_cuisine.keys())
    names = ['Траттория', 'Суши Мастер', 'Пекинская Утка', 'Русская Трапеза', 'Хинкальный Дом',
             'Le Bistro', 'Taco Fiesta', 'Тайский Экспресс', 'Болливуд', 'Burger House']
    restaurants = []
    for i in range(20):
        cuisine = random.choice(cuisines)
        restaurants.append((
            random.choice(names) + f' {i+1}',
            cuisine,
            round(random.uniform(3.5, 4.9), 2),
            fake.address(),
            random.uniform(55.5, 55.9),
            random.uniform(37.3, 37.8)
        ))
    cur.executemany(
        "INSERT INTO delivery.restaurants (name, cuisine, rating, address, latitude, longitude) VALUES (%s,%s,%s,%s,%s,%s) RETURNING restaurant_id",
        restaurants
    )
    conn.commit()
    cur.execute("SELECT restaurant_id, name, cuisine FROM delivery.restaurants")
    restaurants_info = cur.fetchall()

    # 2. Блюда
    dishes = []
    for rest_id, rest_name, cuisine in restaurants_info:
        menu = menu_by_cuisine[cuisine]
        num_dishes = random.randint(10, 20)
        for _ in range(num_dishes):
            dish_name = random.choice(menu)
            dishes.append((
                rest_id,
                dish_name,
                random.choice(categories),
                round(random.uniform(150, 1200), 2)
            ))
    cur.executemany(
        "INSERT INTO delivery.dishes (restaurant_id, name, category, current_price) VALUES (%s,%s,%s,%s)",
        dishes
    )
    conn.commit()

    # 3. Клиенты
    customers = []
    for _ in range(150):
        customers.append((
            fake.name(),
            fake.phone_number(),
            fake.address(),
            random.uniform(55.5, 55.9),
            random.uniform(37.3, 37.8),
            fake.date_time_between(start_date='-1y', end_date='now')
        ))
    cur.executemany(
        "INSERT INTO delivery.customers (name, phone, address, latitude, longitude, registered_at) VALUES (%s,%s,%s,%s,%s,%s)",
        customers
    )
    conn.commit()

    cur.execute("SELECT customer_id FROM delivery.customers")
    customer_ids = [c[0] for c in cur.fetchall()]
    cur.execute("SELECT dish_id, current_price, restaurant_id FROM delivery.dishes")
    dish_data = cur.fetchall()
    dishes_list = [(d[0], float(d[1]), d[2]) for d in dish_data]

    # 4. Заказы
    start_date = datetime.now() - timedelta(days=180)
    orders = []
    items_json = []
    for _ in range(1000):
        restaurant = random.choice(restaurants_info)  # (id, name, cuisine)
        customer = random.choice(customer_ids)
        restaurant_dishes = [d for d in dishes_list if d[2] == restaurant[0]]
        if not restaurant_dishes:
            continue
        num_items = random.randint(1, 5)
        order_items = []
        total = 0
        for _ in range(num_items):
            dish = random.choice(restaurant_dishes)
            qty = random.randint(1, 3)
            price = dish[1]
            total += price * qty
            order_items.append({'dish_id': dish[0], 'quantity': qty, 'price_at_order': price})
        order_time = fake.date_time_between(start_date=start_date, end_date='now')
        delivery_time = order_time + timedelta(minutes=random.randint(20, 90))
        orders.append((restaurant[0], customer, order_time, delivery_time, 'delivered', total))
        items_json.append(json.dumps(order_items))

    cur.executemany(
        "INSERT INTO delivery.orders (restaurant_id, customer_id, order_time, delivery_time, status, total_amount, items) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        [(o[0], o[1], o[2], o[3], o[4], o[5], items_json[i]) for i, o in enumerate(orders)]
    )
    conn.commit()

    # 5. Отзывы (примерно 60% заказов)
    cur.execute("SELECT order_id, items FROM delivery.orders")
    all_orders = cur.fetchall()
    reviews = []
    for order_id, order_items in all_orders[:600]:
        items = order_items  # уже список/словарь
        for item in items:
            if random.random() < 0.7:
                rating = random.randint(3,5) if random.random()<0.8 else random.randint(1,2)
                rev_customer = random.choice(customer_ids)
                reviews.append((item['dish_id'], rev_customer, rating, fake.sentence() if random.random()<0.5 else None))
    if reviews:
        cur.executemany(
            "INSERT INTO delivery.reviews (dish_id, customer_id, rating, comment) VALUES (%s,%s,%s,%s)",
            reviews
        )
        conn.commit()

    cur.close()
    conn.close()
    print("Генерация данных завершена")


def load_raw(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cur.execute("""
        DROP TABLE IF EXISTS raw.raw_orders;
        CREATE TABLE raw.raw_orders AS SELECT * FROM delivery.orders;
        DROP TABLE IF EXISTS raw.raw_restaurants;
        CREATE TABLE raw.raw_restaurants AS SELECT * FROM delivery.restaurants;
        DROP TABLE IF EXISTS raw.raw_dishes;
        CREATE TABLE raw.raw_dishes AS SELECT * FROM delivery.dishes;
        DROP TABLE IF EXISTS raw.raw_reviews;
        CREATE TABLE raw.raw_reviews AS SELECT * FROM delivery.reviews;
        DROP TABLE IF EXISTS raw.raw_customers;
        CREATE TABLE raw.raw_customers AS SELECT * FROM delivery.customers;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Загрузка в raw слой завершена")

def transform(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
    cur.execute("""
        DROP TABLE IF EXISTS staging.order_details;
        CREATE TABLE staging.order_details AS
        SELECT o.order_id, o.restaurant_id, o.customer_id, o.order_time, o.delivery_time,
               EXTRACT(EPOCH FROM (o.delivery_time - o.order_time))/60 AS delivery_time_min,
               o.total_amount,
               jsonb_array_elements(o.items::jsonb) AS item
        FROM raw.raw_orders o WHERE o.status = 'delivered';
    """)
    cur.execute("""
        DROP TABLE IF EXISTS staging.dish_sales;
        CREATE TABLE staging.dish_sales AS
        SELECT order_id, restaurant_id, order_time, delivery_time_min, total_amount,
               (item->>'dish_id')::INTEGER AS dish_id,
               (item->>'quantity')::INTEGER AS quantity,
               (item->>'price_at_order')::DECIMAL(10,2) AS price_at_order,
               ((item->>'quantity')::INTEGER * (item->>'price_at_order')::DECIMAL) AS line_total
        FROM staging.order_details;
    """)
    cur.execute("""
        DROP TABLE IF EXISTS staging.restaurant_aggregates;
        CREATE TABLE staging.restaurant_aggregates AS
        SELECT restaurant_id,
               COUNT(DISTINCT order_id) AS total_orders,
               SUM(total_amount) AS total_revenue,
               AVG(total_amount) AS avg_order_value,
               AVG(delivery_time_min) AS avg_delivery_time_min,
               COUNT(DISTINCT customer_id) AS unique_customers
        FROM staging.order_details
        GROUP BY restaurant_id;
    """)
    cur.execute("""
        DROP TABLE IF EXISTS staging.dish_aggregates;
        CREATE TABLE staging.dish_aggregates AS
        SELECT ds.dish_id, d.name, d.restaurant_id, d.category,
               SUM(ds.quantity) AS total_ordered,
               SUM(ds.line_total) AS revenue,
               AVG(ds.price_at_order) AS avg_price
        FROM staging.dish_sales ds
        JOIN raw.raw_dishes d ON ds.dish_id = d.dish_id
        GROUP BY ds.dish_id, d.name, d.restaurant_id, d.category;
    """)
    cur.execute("""
        DROP TABLE IF EXISTS staging.dish_reviews_agg;
        CREATE TABLE staging.dish_reviews_agg AS
        SELECT dish_id, AVG(rating) AS rating, COUNT(*) AS review_count
        FROM raw.raw_reviews
        GROUP BY dish_id;
    """)
    cur.execute("""
        DROP TABLE IF EXISTS staging.restaurant_reviews_agg;
        CREATE TABLE staging.restaurant_reviews_agg AS
        SELECT d.restaurant_id, AVG(r.rating) AS avg_restaurant_rating, COUNT(r.review_id) AS total_reviews
        FROM raw.raw_reviews r
        JOIN raw.raw_dishes d ON r.dish_id = d.dish_id
        GROUP BY d.restaurant_id;
    """)
    cur.execute("""
        DROP TABLE IF EXISTS staging.delivery_time_by_hour;
        CREATE TABLE staging.delivery_time_by_hour AS
        SELECT EXTRACT(HOUR FROM order_time) AS hour_of_day,
               AVG(delivery_time_min) AS avg_delivery_time,
               COUNT(*) AS order_count
        FROM staging.order_details
        GROUP BY EXTRACT(HOUR FROM order_time)
        ORDER BY hour_of_day;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Трансформация завершена")

def create_mart(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS mart")
    cur.execute("""
        DROP TABLE IF EXISTS mart.restaurant_stats;
        CREATE TABLE mart.restaurant_stats AS
        SELECT r.restaurant_id, r.name, r.cuisine,
               COALESCE(ra.total_orders,0) AS total_orders,
               COALESCE(ra.total_revenue,0) AS total_revenue,
               COALESCE(ra.avg_order_value,0) AS avg_order_value,
               COALESCE(ra.avg_delivery_time_min,0) AS avg_delivery_time_min,
               COALESCE(rra.avg_restaurant_rating, r.rating) AS rating,
               COALESCE(rra.total_reviews,0) AS total_reviews
        FROM raw.raw_restaurants r
        LEFT JOIN staging.restaurant_aggregates ra ON r.restaurant_id = ra.restaurant_id
        LEFT JOIN staging.restaurant_reviews_agg rra ON r.restaurant_id = rra.restaurant_id;
    """)
    cur.execute("""
        DROP TABLE IF EXISTS mart.dish_popularity;
        CREATE TABLE mart.dish_popularity AS
        SELECT d.dish_id, d.name, d.restaurant_id,
               COALESCE(da.total_ordered,0) AS total_ordered,
               COALESCE(da.revenue,0) AS revenue,
               COALESCE(da.avg_price, d.current_price) AS avg_price,
               COALESCE(dra.rating,0) AS rating,
               COALESCE(dra.review_count,0) AS review_count
        FROM raw.raw_dishes d
        LEFT JOIN staging.dish_aggregates da ON d.dish_id = da.dish_id
        LEFT JOIN staging.dish_reviews_agg dra ON d.dish_id = dra.dish_id;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Витрины созданы")

def generate_viz(**context):
    os.makedirs('/opt/airflow/analytics', exist_ok=True)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    # Топ-10 ресторанов
    df = hook.get_pandas_df("SELECT name, total_revenue FROM mart.restaurant_stats ORDER BY total_revenue DESC LIMIT 10")
    fig = go.Figure([go.Bar(x=df['name'], y=df['total_revenue']/1000)])
    fig.update_layout(title='Топ-10 по выручке (тыс. ₽)', xaxis_title='Ресторан', yaxis_title='Выручка')
    fig.write_html('/opt/airflow/analytics/top_restaurants.html')
    # Время доставки по часам
    df2 = hook.get_pandas_df("SELECT hour_of_day, avg_delivery_time FROM staging.delivery_time_by_hour ORDER BY hour_of_day")
    fig2 = go.Figure([go.Scatter(x=df2['hour_of_day'], y=df2['avg_delivery_time'], mode='lines+markers')])
    fig2.update_layout(title='Среднее время доставки по часам', xaxis_title='Час', yaxis_title='Минуты')
    fig2.write_html('/opt/airflow/analytics/delivery_time_by_hour.html')
    # Популярность блюд по категориям
    df3 = hook.get_pandas_df("""
        SELECT d.category, SUM(dp.total_ordered) AS total_sold
        FROM mart.dish_popularity dp
        JOIN delivery.dishes d ON dp.dish_id = d.dish_id
        WHERE d.category IS NOT NULL
        GROUP BY d.category
    """)
    fig3 = px.pie(df3, values='total_sold', names='category', title='Популярность блюд по категориям')
    fig3.write_html('/opt/airflow/analytics/dish_popularity.html')
    # Карта доставки
    df4 = hook.get_pandas_df("""
        SELECT c.latitude, c.longitude, COUNT(o.order_id) AS order_count
        FROM delivery.orders o 
        JOIN delivery.customers c ON o.customer_id = c.customer_id
        WHERE o.status='delivered' AND c.latitude IS NOT NULL
        GROUP BY c.latitude, c.longitude
    """)
    if not df4.empty:
        m = folium.Map(location=[55.751244, 37.618423], zoom_start=11)
        heat_data = [[row['latitude'], row['longitude'], row['order_count']] for _, row in df4.iterrows()]
        HeatMap(heat_data).add_to(m)
        m.save('/opt/airflow/analytics/delivery_map.html')
    print("Визуализации сохранены")


task1 = PythonOperator(task_id='1_generate_data', python_callable=generate_data, dag=dag)
task2 = PythonOperator(task_id='2_load_raw', python_callable=load_raw, dag=dag)
task3 = PythonOperator(task_id='3_transform', python_callable=transform, dag=dag)
task4 = PythonOperator(task_id='4_create_mart', python_callable=create_mart, dag=dag)
task5 = PythonOperator(task_id='5_visualize', python_callable=generate_viz, dag=dag)

task1 >> task2 >> task3 >> task4 >> task5