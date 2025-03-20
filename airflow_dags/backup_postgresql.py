from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
import pendulum
from logger import Logger
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


logger = Logger('file', '/opt/airflow/logs/execucao_tasks_airflow.log')


local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

dag_id = 'backup_postgresql'


with DAG(dag_id,
    schedule_interval='@daily',
    default_args=default_args,
    max_active_runs=1,
    catchup=False
) as dag:
    

    take_backup = BashOperator(
        task_id='take_backup',
        bash_command='sh /opt/airflow/scripts/backup_pg.sh ',
    )

    take_backup
