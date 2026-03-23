#!/bin/bash
set -e

# Выполняем SQL команды, используя psql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Создаем таблицу для логов
    CREATE TABLE user_logs (
        courseid INTEGER,
        userid INTEGER,
        num_week INTEGER,
        s_all INTEGER,
        s_all_avg VARCHAR(255),
        s_course_viewed INTEGER,
        s_course_viewed_avg VARCHAR(255),
        s_q_attempt_viewed INTEGER,
        s_q_attempt_viewed_avg VARCHAR(255),
        s_a_course_module_viewed INTEGER,
        s_a_course_module_viewed_avg VARCHAR(255),
        s_a_submission_status_viewed INTEGER,
        s_a_submission_status_viewed_avg VARCHAR(255),
        NameR_Level VARCHAR(255),
        Name_vAtt VARCHAR(255),
        Depart VARCHAR(255),
        Name_OsnO VARCHAR(255),
        Name_FormOPril VARCHAR(255),
        LevelEd VARCHAR(255),
        Num_Sem INTEGER,
        Kurs INTEGER,
        Date_vAtt VARCHAR(255)
    );


    \copy user_logs FROM '/datasets/aggrigation_logs_per_week.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');


    CREATE TABLE IF NOT EXISTS departments (
        id INTEGER PRIMARY KEY,
        department_name VARCHAR(255) NOT NULL
    );

    TRUNCATE TABLE departments;
    

    \copy departments FROM '/datasets/departments.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');


    ALTER TABLE user_logs ADD COLUMN IF NOT EXISTS depart_id INTEGER;


    UPDATE user_logs SET depart_id = CAST(Depart AS INTEGER);
    

    CREATE OR REPLACE VIEW enriched_user_logs AS
    SELECT 
        u.userid,
        u.courseid,
        d.department_name AS department_name
    FROM user_logs u
    LEFT JOIN departments d ON u.depart_id = d.id;

    SELECT 'Departments loaded:' AS info, COUNT(*) FROM departments;
    SELECT 'User logs updated:' AS info, COUNT(*) FROM user_logs WHERE depart_id IS NOT NULL;

EOSQL

# Выводим результат по заданию
echo "========================================="
echo "ПЕРВЫЕ 10 ЗАПИСЕЙ ПО ЗАДАНИЮ:"
echo "userid | courseid | department_name"
echo "========================================="
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT userid, courseid, department_name 
    FROM enriched_user_logs 
    LIMIT 10;
"