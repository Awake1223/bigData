"""
Витрина данных dmr.analytics_student_performance
Агрегирует активность студентов из public.user_logs.
Структура витрины:
- student_id          INTEGER     ID студента
- course_id           INTEGER     ID курса
- department_id       INTEGER     Код кафедры
- department_name     VARCHAR     Название кафедры
- education_level     VARCHAR     Уровень образования
- education_base      VARCHAR     Основа обучения
- semester            INTEGER     Номер семестра
- course_year         INTEGER     Курс обучения
- final_grade         INTEGER     Итоговая оценка (2-5)
- total_events        INTEGER     Всего событий за семестр
- avg_weekly_events   DECIMAL(10,2) Среднее событий в неделю
- total_course_views  INTEGER     Всего просмотров курса
- total_quiz_views    INTEGER     Всего просмотров тестов
- total_module_views  INTEGER     Всего просмотров модулей
- total_submissions   INTEGER     Всего отправленных заданий
- peak_activity_week  INTEGER     Неделя с максимальной активностью
- consistency_score   DECIMAL(5,2) Коэффициент стабильности активности (0-1)
- activity_category   VARCHAR     Категория активности (низкая/средняя/высокая)
- last_update         TIMESTAMP   Дата обновления записи
"""

# Импорт стандартных библиотек
import os               # для работы с переменными окружения
import sys              # для аварийного выхода при ошибках
from pathlib import Path # для удобной работы с путями файлов

# Импорт сторонних библиотек
from dotenv import load_dotenv   # загрузка .env файла
import psycopg2                  # драйвер PostgreSQL
from psycopg2 import sql          # для безопасной генерации SQL (здесь не критично, но оставлено)
from psycopg2.extras import execute_values  # для массовой вставки кортежей

# -------- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ --------
# Определяем путь к файлу .env как находящемуся в той же папке, что и текущий скрипт
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    # Явно загружаем .env по найденному пути
    load_dotenv(dotenv_path=env_path)
    print(f"✅ Загружен .env из {env_path}")
else:
    # Если файла нет – смысла продолжать нет, выходим с ошибкой
    print(f"❌ Файл .env не найден по пути {env_path}")
    sys.exit(1)

# -------- ФУНКЦИЯ ПОЛУЧЕНИЯ ПАРАМЕТРОВ ПОДКЛЮЧЕНИЯ --------
def get_db_config():
    """Собирает словарь с настройками подключения из переменных окружения."""
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),   # хост БД (по умолчанию localhost)
        'port': os.getenv('DB_PORT', '5432'),         # порт (по умолч. 5432)
        'database': os.getenv('DB_NAME', 'educational_portal'), # имя БД
        'user': os.getenv('DB_USER', 'postgres'),     # пользователь
        'password': os.getenv('DB_PASSWORD', '')      # пароль
    }
    # Проверка: пароль не должен быть пустым
    if not config['password']:
        print("❌ Пароль не задан! Проверьте DB_PASSWORD в .env")
        sys.exit(1)
    # Выводим параметры, скрывая пароль (для отладки)
    print("Параметры подключения:", {**config, 'password': '***'})
    return config

# -------- ФУНКЦИЯ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ --------
def get_connection():
    """Устанавливает соединение с PostgreSQL и возвращает объект connection."""
    try:
        config = get_db_config()
        conn = psycopg2.connect(**config)   # открываем соединение
        conn.autocommit = False             # отключаем автокоммит – будем управлять вручную
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

# -------- СОЗДАНИЕ СХЕМЫ dmr --------
def create_schema(conn):
    """Создаёт схему dmr, если её ещё нет."""
    with conn.cursor() as cur:          # открываем курсор (автоматически закроется по выходу из with)
        cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
        conn.commit()                   # фиксируем изменение
        print("Схема dmr создана.")

# -------- СОЗДАНИЕ ТАБЛИЦЫ ВИТРИНЫ --------
def create_table(conn):
    """Создаёт таблицу dmr.analytics_student_performance со всеми необходимыми полями."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance (
        student_id          INTEGER NOT NULL,        -- ID студента
        course_id           INTEGER NOT NULL,        -- ID курса
        department_id       INTEGER,                 -- код кафедры
        department_name     VARCHAR(255),            -- название кафедры
        education_level     VARCHAR(50),             -- уровень образования
        education_base      VARCHAR(50),             -- основа обучения (бюджет/платно)
        semester            INTEGER,                 -- номер семестра
        course_year         INTEGER,                 -- курс обучения (1,2,3...)
        final_grade         INTEGER CHECK (final_grade IN (2,3,4,5)), -- итоговая оценка
        total_events        INTEGER DEFAULT 0,       -- всего событий за семестр
        avg_weekly_events   DECIMAL(10,2),           -- среднее событий в неделю
        total_course_views  INTEGER DEFAULT 0,       -- просмотров страниц курса
        total_quiz_views    INTEGER DEFAULT 0,       -- просмотров тестов
        total_module_views  INTEGER DEFAULT 0,       -- просмотров модулей
        total_submissions   INTEGER DEFAULT 0,       -- отправленных заданий
        peak_activity_week  INTEGER,                 -- неделя с пиком активности
        consistency_score   DECIMAL(5,2),            -- стабильность активности (0..1)
        activity_category   VARCHAR(20),             -- категория: низкая/средняя/высокая
        last_update         TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- время обновления записи
        PRIMARY KEY (student_id, course_id, semester) -- уникальность: студент+курс+семестр
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        print("Таблица dmr.analytics_student_performance создана.")

# -------- ЗАПОЛНЕНИЕ ВИТРИНЫ ДАННЫМИ --------
def insert_data(conn):
    """
    Агрегирует данные из public.user_logs (уже понедельная статистика)
    и вставляет результат в витрину.
    """
    # Основной SQL-запрос агрегации
    agg_query = """
    WITH weekly_data AS (
        -- Извлекаем все нужные поля из user_logs, приводим типы, заменяем NULL на 0
        SELECT
            userid AS student_id,
            courseid AS course_id,
            depart_id AS department_id,
            depart AS department_name,
            name_formopril AS education_level,
            name_osno AS education_base,
            num_sem AS semester,
            kurs AS course_year,
            CAST(namer_level AS INTEGER) AS final_grade,   -- оценка как число
            num_week,
            COALESCE(s_all, 0) AS s_all,                  -- общее число событий за неделю
            COALESCE(s_course_viewed, 0) AS s_course_viewed,
            COALESCE(s_q_attempt_viewed, 0) AS s_q_attempt_viewed,
            COALESCE(s_a_course_module_viewed, 0) AS s_a_course_module_viewed,
            COALESCE(s_a_submission_status_viewed, 0) AS s_a_submission_status_viewed
        FROM public.user_logs
        WHERE namer_level IS NOT NULL           -- только записи с оценкой
          AND namer_level ~ '^[2-5]$'           -- оценка от 2 до 5 (регулярное выражение)
          AND num_sem IS NOT NULL               -- семестр должен быть заполнен
    ),
    student_course_agg AS (
        -- Группируем по студенту, курсу и семестру, вычисляем агрегаты
        SELECT
            student_id,
            course_id,
            MAX(department_id) AS department_id,
            MAX(department_name) AS department_name,
            MAX(education_level) AS education_level,
            MAX(education_base) AS education_base,
            semester,
            MAX(course_year) AS course_year,
            MAX(final_grade) AS final_grade,
            SUM(s_all) AS total_events,                     -- всего событий
            AVG(s_all)::DECIMAL(10,2) AS avg_weekly_events, -- среднее за неделю
            SUM(s_course_viewed) AS total_course_views,
            SUM(s_q_attempt_viewed) AS total_quiz_views,
            SUM(s_a_course_module_viewed) AS total_module_views,
            SUM(s_a_submission_status_viewed) AS total_submissions,
            -- Неделя с максимальным числом событий (при равенстве – берём меньшую неделю)
            (array_agg(num_week ORDER BY s_all DESC, num_week ASC))[1] AS peak_activity_week,
            MAX(s_all) AS max_weekly_events,
            COUNT(*) AS weeks_count                          -- сколько недель данных
        FROM weekly_data
        GROUP BY student_id, course_id, semester
    )
    -- Финальный SELECT: добавляем вычисляемые метрики (consistency_score, category)
    SELECT
        student_id,
        course_id,
        department_id,
        department_name,
        education_level,
        education_base,
        semester,
        course_year,
        final_grade,
        total_events,
        avg_weekly_events,
        total_course_views,
        total_quiz_views,
        total_module_views,
        total_submissions,
        peak_activity_week,
        -- Коэффициент стабильности = среднее / максимум (если максимум > 0)
        CASE
            WHEN max_weekly_events > 0 THEN ROUND(avg_weekly_events / max_weekly_events, 2)
            ELSE 0
        END AS consistency_score,
        -- Категория активности на основе средних событий в неделю
        CASE
            WHEN avg_weekly_events <= 5 THEN 'низкая'
            WHEN avg_weekly_events <= 20 THEN 'средняя'
            ELSE 'высокая'
        END AS activity_category
    FROM student_course_agg
    WHERE final_grade IN (2,3,4,5);  -- только строки с валидной оценкой
    """

    # SQL для вставки (или обновления) в витрину
    insert_query = sql.SQL("""
        INSERT INTO dmr.analytics_student_performance (
            student_id, course_id, department_id, department_name,
            education_level, education_base, semester, course_year,
            final_grade, total_events, avg_weekly_events,
            total_course_views, total_quiz_views, total_module_views,
            total_submissions, peak_activity_week, consistency_score,
            activity_category
        )
        VALUES %s
        ON CONFLICT (student_id, course_id, semester)   -- если такая пара уже есть
        DO UPDATE SET                                   -- обновляем все поля, кроме первичного ключа
            department_id        = EXCLUDED.department_id,
            department_name      = EXCLUDED.department_name,
            education_level      = EXCLUDED.education_level,
            education_base       = EXCLUDED.education_base,
            course_year          = EXCLUDED.course_year,
            final_grade          = EXCLUDED.final_grade,
            total_events         = EXCLUDED.total_events,
            avg_weekly_events    = EXCLUDED.avg_weekly_events,
            total_course_views   = EXCLUDED.total_course_views,
            total_quiz_views     = EXCLUDED.total_quiz_views,
            total_module_views   = EXCLUDED.total_module_views,
            total_submissions    = EXCLUDED.total_submissions,
            peak_activity_week   = EXCLUDED.peak_activity_week,
            consistency_score    = EXCLUDED.consistency_score,
            activity_category    = EXCLUDED.activity_category,
            last_update          = CURRENT_TIMESTAMP;   -- обновляем время
    """)

    with conn.cursor() as cur:
        # Выполняем агрегирующий запрос, получаем строки для вставки
        cur.execute(agg_query)
        rows = cur.fetchall()
        if not rows:
            print("Нет данных для вставки (возможно, нет записей с оценками 2-5).")
            return

        # Преобразуем список строк в список кортежей (execute_values ожидает именно кортежи)
        data_tuples = [tuple(row) for row in rows]
        # Массовая вставка блоками по 1000 строк (повышает скорость)
        execute_values(cur, insert_query, data_tuples, page_size=1000)
        conn.commit()   # фиксируем транзакцию
        # cur.rowcount возвращает общее количество обработанных строк (вставлено+обновлено)
        print(f"Витрина заполнена. Добавлено/обновлено записей: {cur.rowcount}")

# -------- ГЛАВНАЯ ФУНКЦИЯ --------
def main():
    """Последовательно выполняет шаги: подключение, создание схемы, таблицы, вставка."""
    conn = None
    try:
        conn = get_connection()        # 1. Подключаемся к БД
        create_schema(conn)            # 2. Создаём схему dmr
        create_table(conn)             # 3. Создаём таблицу витрины
        insert_data(conn)              # 4. Загружаем данные
        print("\n✅ Все операции выполнены успешно!")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        if conn:
            conn.rollback()            # откатываем все изменения при ошибке
    finally:
        if conn:
            conn.close()               # в любом случае закрываем соединение
            print("Соединение закрыто.")

# Точка входа в программу
if __name__ == "__main__":
    main()