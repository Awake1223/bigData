
import os
import sys
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

"""
def get_db_config():
    
    config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    return config 
"""


def get_db_config():
    """Временная конфигурация для теста"""
    return {
        'host': 'host.docker.internal',
        'port': 5433,
        'database': 'my_db_sviridov',
        'user': 'zhora',
        'password': 'Zh0r@2026!SeCr3tP@ss'  # ваш пароль
    }


def get_connection():
    """Устанавливает соединение с PostgreSQL и возвращает объект connection."""
    try:
        config = get_db_config()
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)


def create_schema(conn):
    """Создаёт схему dmr, если её ещё нет."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
        conn.commit()
        print("Схема dmr создана.")


def create_table(conn):
    """Создаёт таблицу витрины со всеми необходимыми полями."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance (
        student_id          INTEGER NOT NULL,
        course_id           INTEGER NOT NULL,
        department_id       INTEGER,
        department_name     VARCHAR(255),
        education_level     VARCHAR(50),
        education_base      VARCHAR(50),
        semester            INTEGER,
        course_year         INTEGER,
        final_grade         INTEGER CHECK (final_grade IN (2,3,4,5)),
        total_events        INTEGER DEFAULT 0,
        avg_weekly_events   DECIMAL(10,2),
        total_course_views  INTEGER DEFAULT 0,
        total_quiz_views    INTEGER DEFAULT 0,
        total_module_views  INTEGER DEFAULT 0,
        total_submissions   INTEGER DEFAULT 0,
        peak_activity_week  INTEGER,
        consistency_score   DECIMAL(5,2),
        activity_category   VARCHAR(20),
        last_update         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (student_id, course_id, semester)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        print("Таблица dmr.analytics_student_performance создана.")


def insert_data(conn):
    """Агрегирует данные и вставляет в витрину."""
    agg_query = """
    WITH weekly_data AS (
        SELECT
            userid AS student_id,
            courseid AS course_id,
            depart_id AS department_id,
            depart AS department_name,
            name_formopril AS education_level,
            name_osno AS education_base,
            num_sem AS semester,
            kurs AS course_year,
            CAST(namer_level AS INTEGER) AS final_grade,
            num_week,
            COALESCE(s_all, 0) AS s_all,
            COALESCE(s_course_viewed, 0) AS s_course_viewed,
            COALESCE(s_q_attempt_viewed, 0) AS s_q_attempt_viewed,
            COALESCE(s_a_course_module_viewed, 0) AS s_a_course_module_viewed,
            COALESCE(s_a_submission_status_viewed, 0) AS s_a_submission_status_viewed
        FROM public.user_logs
        WHERE namer_level IS NOT NULL
          AND namer_level ~ '^[2-5]$'
          AND num_sem IS NOT NULL
    ),
    student_course_agg AS (
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
            SUM(s_all) AS total_events,
            AVG(s_all)::DECIMAL(10,2) AS avg_weekly_events,
            SUM(s_course_viewed) AS total_course_views,
            SUM(s_q_attempt_viewed) AS total_quiz_views,
            SUM(s_a_course_module_viewed) AS total_module_views,
            SUM(s_a_submission_status_viewed) AS total_submissions,
            (array_agg(num_week ORDER BY s_all DESC, num_week ASC))[1] AS peak_activity_week,
            MAX(s_all) AS max_weekly_events,
            COUNT(*) AS weeks_count
        FROM weekly_data
        GROUP BY student_id, course_id, semester
    )
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
        CASE
            WHEN max_weekly_events > 0 THEN ROUND(avg_weekly_events / max_weekly_events, 2)
            ELSE 0
        END AS consistency_score,
        CASE
            WHEN avg_weekly_events <= 5 THEN 'низкая'
            WHEN avg_weekly_events <= 20 THEN 'средняя'
            ELSE 'высокая'
        END AS activity_category
    FROM student_course_agg
    WHERE final_grade IN (2,3,4,5);
    """

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
        ON CONFLICT (student_id, course_id, semester)
        DO UPDATE SET
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
            last_update          = CURRENT_TIMESTAMP;
    """)

    with conn.cursor() as cur:
        cur.execute(agg_query)
        rows = cur.fetchall()
        if not rows:
            print("Нет данных для вставки.")
            return

        data_tuples = [tuple(row) for row in rows]
        execute_values(cur, insert_query, data_tuples, page_size=1000)
        conn.commit()
        print(f"Витрина заполнена. Добавлено/обновлено записей: {cur.rowcount}")


def main():
    """Основная функция создания витрины."""
    conn = None
    try:
        conn = get_connection()
        create_schema(conn)
        create_table(conn)
        insert_data(conn)
        print("\n✅ Витрина успешно создана/обновлена!")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Соединение закрыто.")


if __name__ == "__main__":
    main()