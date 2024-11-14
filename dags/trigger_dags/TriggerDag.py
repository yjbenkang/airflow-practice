from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

dag = DAG(
    dag_id='SourceDag',
    start_date=datetime(2023, 6, 19),
    schedule='@daily'
)

trigger_task = TriggerDagRunOperator(
    task_id='trigger_task',
    trigger_dag_id='TargetDag',
    conf={'path': 'value1'},
    execution_date='{{ ds }}',
    reset_dag_run=True,
    dag=dag
)
