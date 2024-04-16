from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from db import extract_data, create_database, create_star_schema, insertar_datos_star_schema
from transform import convert_irca_columns, scale_columns, standardize_column_names, classify_irca, categorize_treatment

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'water_quality_etl_dag',
    default_args=default_args,
    description='ETL DAG for Water Quality Data',
    schedule_interval=timedelta(days=1),
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data,
        op_kwargs={'filename': 'db_config.json', 'db_name': 'db_water', 'table_name': 'tabla_data_cleaned'}
    )

    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=convert_irca_columns,
        # Add more transformation functions if necessary, you can chain them inside the callable if required
    )

    create_database_task = PythonOperator(
        task_id='create_database_task',
        python_callable=create_database,
        op_kwargs={'config_file': 'db_config.json', 'db_name': 'db_star_schema'}
    )

    create_star_schema_task = PythonOperator(
        task_id='create_star_schema_task',
        python_callable=create_star_schema,
        op_kwargs={'config_filename': 'db_config.json', 'db_name': 'db_star_schema'}
    )

    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=insertar_datos_star_schema,
        # Ensure you pass the transformed data correctly, possibly using XCom or another mechanism
    )


    extract_data_task >> transform_data_task
    transform_data_task >> create_database_task
    create_database_task >> create_star_schema_task
    create_star_schema_task >> load_data_task

