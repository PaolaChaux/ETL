from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
import etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'water',
    default_args=default_args,
    description='ETL process for water quality data analysis',
    schedule_interval=timedelta(days=1),
)

with dag:
    read_water = PythonOperator(
        task_id='read_water',
        python_callable=etl.read_water,
        provide_context=True,
    )

    read_api = PythonOperator(
        task_id='extract_api',
        python_callable=etl.read_api,
        provide_context=True,
    )

    transform_water = PythonOperator(
        task_id='transform_water',
        python_callable=etl.transform_water,
        provide_context=True,
    )

    transform_api = PythonOperator(
        task_id='transform_api',
        python_callable=etl.transform_api,
        provide_context=True,
    )

    merge_task = PythonOperator(
        task_id='merge',
        python_callable=etl.merge_task,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=etl.load,
        provide_context=True,
    )

  


    read_water >> transform_water
    read_api >> transform_api
    [transform_water, transform_api] >> merge_task >> load_task