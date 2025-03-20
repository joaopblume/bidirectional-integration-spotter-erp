from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.exceptions import AirflowSkipException
import pendulum
import os
from logger import Logger


logger = Logger('file', '/opt/airflow/logs/execucao_tasks_airflow.log')

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
}

dag_id = 'job_execute_spark'


'''
    Esta dag executa os notebooks no sparks, que geram inserçoes e atualizacoes para o CRM.
'''

def log_inicio():
    logger.log({'msg': 'Iniciando execucao dos notebooks no spark', 'dag_id': dag_id})



'''
    Funcao que verifica se existem novos arquivos gerados pelo spark.
'''
def check_new_files(**kwargs):
    logger.log({'msg': 'Buscando novos arquivos criados pelo spark', 'dag_id': dag_id})
    source_leads_directory = '/data/integration/leads/processar'
    source_contacts_directory = '/data/integration/contacts/processar'


    for source_directory in [source_leads_directory, source_contacts_directory]:
        json_files = [f for f in os.listdir(source_directory) if f.endswith('.json')]
        if not json_files:
            logger.log({'msg': 'Nenhum json novo foi gerado', 'dag_id': dag_id})
            raise AirflowSkipException("Não foram encontrados updates de leads pendentes de sincronização.")
        
        else:
            return True


with DAG(dag_id,
            schedule_interval="*/15 * * * *",
            default_args=default_args,
            max_active_runs=1,
            catchup=False) as dag:


    # Executa o spark
    gera_filas_spark = SparkSubmitOperator(
        task_id='gera_filas_spark',
        conn_id='spark_conn',
        application='/opt/spark/scripts/main_integration.py',
        executor_memory='512m',
        total_executor_cores=1,
        jars='/opt/spark/drivers/jdbcdriver/ojdbc8.jar',
        env_vars={"PYSPARK_PYTHON": "/usr/bin/python3.8"}
    )

    check_new_files = PythonOperator(
        task_id='verifica_novas_filas_geradas',
        python_callable=check_new_files
    )

    log_start = PythonOperator(
        task_id='start_processes',
        python_callable=log_inicio
    )

    log_start >> gera_filas_spark >> check_new_files
