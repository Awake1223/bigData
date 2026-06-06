from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))
from build_performance_mart import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'create_performance_mart',
    default_args=default_args,
    description='Создание витрины dmr.analytics_student_performance',
    schedule_interval=None,
    catchup=False,
    tags=['mart', 'performance'],
) as dag:

    create_mart_task = PythonOperator(
        task_id='create_performance_mart',
        python_callable=main,
    )

    create_mart_task