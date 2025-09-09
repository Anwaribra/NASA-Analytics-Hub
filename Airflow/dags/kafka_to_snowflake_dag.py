from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import sys
import asyncio
import logging
from Kafka.producers.eonet_producer import run as run_eonet
from Kafka.producers.neo_producer import run as run_neo
from Kafka.producers.iss_producer import run as run_iss
from Kafka.consumers.snowflake_consumer import consume as run_consumer

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['anwaribrrahim@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nasa_analytics_pipeline',
    default_args=default_args,
    description='Pipeline to process NASA data through Kafka and Snowflake',
    schedule_interval='0 0 * * *',   
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nasa', 'kafka', 'snowflake', 'ml'],
)
def run_async_task(async_func):
    asyncio.run(async_func())

# producers
with TaskGroup(group_id='producers', dag=dag) as producer_group: 
    eonet_producer = PythonOperator(
        task_id='eonet_producer',
        python_callable=run_async_task,
        op_args=[run_eonet],
        dag=dag,
    )
    neo_producer = PythonOperator(
        task_id='neo_producer',
        python_callable=run_async_task,
        op_args=[run_neo],
        dag=dag,
    )
    iss_producer = PythonOperator(
        task_id='iss_producer',
        python_callable=run_async_task,
        op_args=[run_iss],
        dag=dag,
    )
# consumer
consumer = PythonOperator(
    task_id='kafka_consumer',
    python_callable=run_async_task,
    op_args=[run_consumer],
    dag=dag,
)

# DBT 
with TaskGroup(group_id='dbt_transforms', dag=dag) as dbt_group:

    dbt_stage = BashOperator(
        task_id='dbt_stage',
        bash_command='dbt run --models staging',
        env={
            'DBT_PROFILES_DIR': '{{ var.value.dbt_profiles_dir }}',
            'DBT_PROJECT_DIR': '{{ var.value.dbt_project_dir }}'
        },
        dag=dag,
    )

    dbt_warehouse = BashOperator(
        task_id='dbt_warehouse',
        bash_command='dbt run --models warehouse',
        env={
            'DBT_PROFILES_DIR': '{{ var.value.dbt_profiles_dir }}',
            'DBT_PROJECT_DIR': '{{ var.value.dbt_project_dir }}'
        },
        dag=dag,
    )


    dbt_marts = BashOperator(
        task_id='dbt_marts',
        bash_command='dbt run --models marts',
        env={
            'DBT_PROFILES_DIR': '{{ var.value.dbt_profiles_dir }}',
            'DBT_PROJECT_DIR': '{{ var.value.dbt_project_dir }}'
        },
        dag=dag,
    )

    dbt_stage >> dbt_warehouse >> dbt_marts

# ML
with TaskGroup(group_id='ml_pipeline', dag=dag) as ml_group:
    event_classification = BashOperator(
        task_id='event_classification',
        bash_command='python {{ var.value.project_root }}/ML/event_classification.ipynb',
        dag=dag,
    )

    asteroid_forecast = BashOperator(
        task_id='asteroid_forecast',
        bash_command='python {{ var.value.project_root }}/ML/asteroid_forecast.ipynb',
        dag=dag,
    )

producer_group >> consumer >> dbt_group >> ml_group

# #  monitoring 
# def task_failure_callback(context):
#     """Send alert on task failure"""
#     task_instance = context['task_instance']
#     task_name = task_instance.task_id
#     dag_id = task_instance.dag_id
#     exec_date = context['execution_date']
#     exception = context['exception']
    
#     error_message = f"""
#     Task Failed!
#     DAG: {dag_id}
#     Task: {task_name}
#     Execution Date: {exec_date}
#     Exception: {str(exception)}
#     """
    
#     logging.error(error_message)
  
# default_args['on_failure_callback'] = task_failure_callback
