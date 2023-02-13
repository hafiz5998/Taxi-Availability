from __future__ import annotations

import datetime

import pendulum

from airflow import DAG,XComArg
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
import json
def openJson(**kwargs):
    with open('/home/hafizaimanhassan/airflow/image/api_bq/alert.json', 'r') as openAlert:
                cityAlert = json.load(openAlert)
                kwargs['ti'].xcom_push(key='val', value=cityAlert)
def getString(ti):
    a = ti.xcom_pull(key='val', task_ids='alertJson')
    return a


with DAG(
    dag_id='SG_TAXI_DAG_HISTORY',
    schedule='*/60 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
) as dag:
    ingestData = BashOperator(
        task_id='ingestData',
        bash_command='python3 /home/hafizaimanhassan/airflow/image/api_bq/main_append.py',
        do_xcom_push=True
    )


ingestData 


if __name__ == "__main__":
    dag.cli()