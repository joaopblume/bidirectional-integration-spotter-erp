from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.oracle.hooks.oracle import OracleHook
import pendulum
import os
import requests
import json
import shutil
from logger import Logger

logger = Logger('file', '/opt/airflow/logs/execucao_tasks_airflow.log')

dag_id = 'job_insert_contacts_in_crm'

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

def log_inicio():
    logger.log({'msg': 'Iniciando inserção de contatos no CRM', 'dag_id': dag_id})
    

def list_files(**kwargs):
    source_directory = '/data/integration/contacts/processar'
    json_files = [f for f in os.listdir(source_directory) if f.startswith('insert_contact_crm') and f.endswith('.json')]
    if not json_files:
        raise AirflowSkipException("Não foram encontrados inserts de contatos pendentes de sincronização.")

    kwargs['ti'].xcom_push(key='insert_contact_files', value=json_files)


def atualiza_crm_id(crm_id, email):
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    row = (email, crm_id)
    oracle_hook.callproc('PRC_GRAVA_ID_CRM_CONT_CLI', autocommit=True, parameters=row)

def insert_in_crm(**kwargs):
    contador = 0
    for file in kwargs['ti'].xcom_pull(key='insert_contact_files'):
        with open(f'/data/integration/contacts/processar/{file}', 'r') as f:
            content = f.readlines()
            for line in content:
                body = json.loads(line)
                headers = {'Content-Type': 'application/json'}
                url = f"http://api.example.com:5000/inserecontato"

                for key, value in body.items():
                    if value == None:
                        print(f'Valor: {key} esta nulo')

                print(body)
                logger.log({'msg': 'Fazendo INSERT do Contato no CRM API', 'dag_id': dag_id})

                response = requests.post(url, headers=headers, data=json.dumps(body))
                if response.status_code != 200:
                    logger.log({'msg': 'Nao foi possivel enviar o POST para o CRM API', 'dag_id': dag_id})

                    raise requests.HTTPError(f'Não foi possivel INSERIR o contato: {response.text}')
                else:
                    crm_id = response.json()['value']
                    email = body['email']
                    atualiza_crm_id(crm_id, email)
                    contador += 1
                    print('Registro atualizado com sucesso!')
            
    return f'{contador} registros atualizados com sucesso!'


def move_json_files(**kwargs):
    logger.log({'msg': 'Movendo os arquivos processados', 'dag_id': dag_id})

    ti = kwargs['ti']
    json_files = ti.xcom_pull(key='insert_contact_files')
    if json_files is None or not json_files:
        print("Nenhum arquivo JSON para mover.")
        return
    source_directory = '/data/integration/contacts/processar'
    destination_directory = '/data/integration/contacts/processados'
    for json_file in json_files:
        source = os.path.join(source_directory, json_file)
        destination = os.path.join(destination_directory, json_file)
        shutil.move(source, destination)
        print(f"Arquivo {json_file} movido para {destination}")
            

with DAG(dag_id,
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         max_active_runs=1,
         catchup=False) as dag:
    

    list_files = PythonOperator(
        task_id = 'task_lista_arquivos_pendentes',
        python_callable=list_files,
        provide_context=True
    )

    update_on_crm = PythonOperator(
        task_id = 'task_atualiza_contato_no_crm',
        python_callable=insert_in_crm,
        provide_context=True
    )

    move_files = PythonOperator(
        task_id = 'move_processed_files',
        python_callable=move_json_files,
        provide_context=True
    )

    log_start = PythonOperator(
        task_id='start_processes',
        python_callable=log_inicio
    )

    log_start >> list_files >> update_on_crm >> move_files
