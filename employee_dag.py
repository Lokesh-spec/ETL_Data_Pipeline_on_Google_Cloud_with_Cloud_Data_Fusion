from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': ['lokeshkv18@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('employee_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    extract_datapipeline = BashOperator(
        task_id='extract_employee_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract_employee_data.py',
    )

    transform_datapipeline= CloudDataFusionStartPipelineOperator(
        location="us-west1",
        pipeline_name="employee-etl-pipeline",
        instance_name="employee-datafusion-dev",
        task_id="employee_data_transform_pipeline",
    )

    extract_datapipeline >> transform_datapipeline