from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import tasks.raizen_etl as re

default_args = {
    "owner": "Matheus",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 19),
}

dag = DAG("etl-raizen", default_args=default_args, schedule_interval=None)

derivados = PythonOperator(
    task_id='Rodando-derivados',
    python_callable=re.rodar_transformacoes,
    op_kwargs={'nome': ['derivados'],'folhas' : [1]},
    dag=dag
)

diesel = PythonOperator(
    task_id='Rodando-Diesel',
    python_callable=re.rodar_transformacoes,
    op_kwargs={'nome': ['diesel'],'folhas' : [2]},
    dag=dag
)

derivados >> diesel