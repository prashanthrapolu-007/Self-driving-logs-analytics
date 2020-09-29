from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import MoveFileToS3
from helpers import SQLQueries

default_args = {
    "owner": "Prashanth",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 25),
    "email": ['etl_job_owner@uber.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15)
}

with DAG('set_up_for_log_data', default_args=default_args, schedule_interval='@once',
         max_active_runs=1) as dag:
    start_task = DummyOperator(
        task_id='Dummy_Start'
    )

    move_spark_script_to_s3 = MoveFileToS3(
        task_id='move_pyspark_script_to_s3',
        bucket_name=os.getenv('SCRIPTS_BUCKET'),
        file_name=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts/spark'
                                                                           '/transform_log_data_for_analytics.py'),
        key='spark/transform_log_data_for_analytics.py'
    )

    set_up_redshift_table = PostgresOperator(
        task_id='create_table',
        sql=SQLQueries.create_redshift_table,
        postgres_conn_id='redshift_conn'
    )

start_task >> move_spark_script_to_s3 >> set_up_redshift_table

