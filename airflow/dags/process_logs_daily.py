import json
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from operators import S3ToRedshiftOperator
from operators import DynamicEMRStepsOperator


default_args = {
    "owner": "Prashanth",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 27),
    "email": ['etl_job_owner@uber.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15)
}


with open('/home/nani/PycharmProjects/Uber_log_analytics_ETL/airflow/dags/scripts/emr/process_log_file_emr.json') as json_file:
    emr_steps = json.load(json_file)

last_step = len(emr_steps) - 1
JOB_FLOW_OVERRIDES = {
    'Name': 'Uber-log-analytics'
}

with DAG('process_and_load_log_data_into_redshift', default_args=default_args, schedule_interval='@once',
         max_active_runs=1) as dag:

    start_task = DummyOperator(
        task_id='dummy_start'
    )

    spin_aws_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default'
    )

    add_emr_steps = DynamicEMRStepsOperator(
        task_id='add_emr_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        steps=emr_steps,
        params={
            'scripts_bucket': os.getenv('SCRIPTS_BUCKET'),
            'logs_bucket': os.getenv('LOGS_BUCKET'),
            'raw_logs_folder': '/raw_logs/',
            'processed_logs_folder': '/processed_logs/'
        },
        depends_on_past=True
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id='{{ task_instance.xcom_pull("add_emr_steps", key="return_value")[' + str(
            last_step) + '] }}',
        aws_conn_id='aws_default'
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default"
    )

    load_data_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_data_from_s3_to_redshift",
        redshift_conn_id="reshift_conn",
        s3_bucket=os.getenv('LOGS_BUCKET'),
        s3_folder_path="processed_logs/",
        schema="public",
        table=os.getenv('REDSHIFT_TABLE'),
        delimiter=',',
        region=os.getenv('REDSHIFT_REGION'),
        s3_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
        s3_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY')

    )

start_task >> spin_aws_emr_cluster >> add_emr_steps >> step_checker >> remove_cluster >> load_data_to_redshift
