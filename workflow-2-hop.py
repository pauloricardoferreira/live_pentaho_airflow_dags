#Roda o carte.job
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow_pentaho.operators.carte import CarteJobOperator

local_tz=pendulum.timezone('America/Sao_Paulo')

default_args = {
    'owner': 'Dono',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(days=1),  #datetime(2021, 3, 13, 0, tzinfo=local_tz), datetime(yyyy,mm,dd,hh,mn,sc, tzinfo=local_tz),
    'email': ['admin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    dag_id='dag-workflow-2-hop',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['docker', 'hop']
)

job = CarteJobOperator(
    dag=dag,
    task_id='tsk-workflow-2-hop',
    job='INTEGRACAO/workflow',
    level= 'Basic'
)
