from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
import pendulum
import os
import requests
import json
import shutil
from logger import Logger

logger = Logger('file', '/opt/airflow/logs/execucao_tasks_airflow.log')

dag_id = 'job_update_contacts_in_crm'

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

def log_inicio():
    logger.log({'msg': 'Iniciando atualizacao de contatos no CRM', 'dag_id': dag_id})
    

def list_files(**kwargs):
    source_directory = '/data/integration/contacts/processar'
    json_files = [f for f in os.listdir(source_directory) if f.startswith('update_contacts_in_crm') and f.endswith('.json')]
    if not json_files:
        raise AirflowSkipException("Não foram encontrados updates de contatos pendentes de sincronização.")

    kwargs['ti'].xcom_push(key='updated_contact_files', value=json_files)
    

def atualiza_obs_lead(lead_id):
    url = f'http://api.example.com:5000/contatoobs/{lead_id}/N'
    response = requests.post(url)
    if response.status_code != 201:
        raise requests.HTTPError(f'Não foi possivel atualizar a observacao da lead {lead_id}: {response.text}')
    else:
        return True


def update_on_crm(**kwargs):
    headers = {'Content-Type': 'application/json'}
    contador = 0
    for file in kwargs['ti'].xcom_pull(key='updated_contact_files'):
        with open(f'/data/integration/contacts/processar/{file}', 'r') as f:
            content = f.readlines()
            for line in content:
                fields = {}
                file_data = json.loads(line)
                for key, value in file_data.items():
                    
                    fields[key.lower()] = value
                
                body = {}
                print(fields)

                try:
                    body['name'] = fields['name']
                except KeyError as e:
                    raise e
                
                try:
                    body['email'] = fields['email'].lower()
                except KeyError as e:
                    raise e
                try:
                    body['jobTitle'] = fields['jobtitle']
                except KeyError as e:
                    raise e
                try:
                    body['phone1'] = str(fields['phone1']).split('.')[0]
                except KeyError as e:
                    print('Phone1 esta nulo')
                    body['phone1'] = '5500000000'
                try:
                    lead_id = int(fields['lead_id'])
                except KeyError as e:
                    continue
                try:
                    mainContact = fields['maincontact']
                    if mainContact == 'S':
                        body['mainContact'] = True
                    elif mainContact == 'N':
                        body['mainContact'] = False
                    else:
                        raise ValueError(f'Valor invalido para mainContact: {mainContact}')
                except KeyError as e:
                    raise e
                
                contact_id = int(fields['id'])
                url = f"http://api.example.com:5000/atualizacontato/{contact_id}"

                for key, value in body.items():
                    if value == None:
                        print(f'Valor: {key} esta nulo')

                print(body)
                logger.log({'msg': 'Fazendo PUT da Contato no CRM API', 'dag_id': dag_id})

                response = requests.post(url, headers=headers, data=json.dumps(body))
                if response.status_code != 200:
                    logger.log({'msg': 'Nao foi possivel enviar o PUT para o CRM API', 'dag_id': dag_id})

                    raise requests.HTTPError(f'Não foi possivel atualizar o registro id {contact_id}: {response.text}')
                else:
                    obs_atualizado = atualiza_obs_lead(lead_id)
                    if obs_atualizado:
                        contador += 1
                        print('Registro atualizado com sucesso!')
                    else:
                        print(f'Erro ao atualizar a observacao da lead {lead_id}')
                        raise requests.HTTPError(f'Erro ao atualizar a observacao da lead {lead_id}')
            
            return f'{contador} registros atualizados com sucesso!'


def move_json_files(**kwargs):
    logger.log({'msg': 'Movendo os arquivos processados', 'dag_id': dag_id})

    ti = kwargs['ti']
    json_files = ti.xcom_pull(key='updated_contact_files')
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
         schedule_interval='*/10 * * * *',
         max_active_runs=1,
         catchup=False) as dag:
    

    list_files = PythonOperator(
        task_id = 'task_lista_arquivos_pendentes',
        python_callable=list_files,
        provide_context=True
    )

    update_on_crm = PythonOperator(
        task_id = 'task_atualiza_contato_no_crm',
        python_callable=update_on_crm,
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
