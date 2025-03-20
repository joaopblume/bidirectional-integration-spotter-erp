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

dag_id = 'job_update_leads_in_crm'

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

def log_inicio():
    logger.log({'msg': 'Iniciando atualizacao de leads no CRM', 'dag_id': dag_id})
    

def list_files(**kwargs):
    source_directory = '/data/integration/leads/processar'
    json_files = [f for f in os.listdir(source_directory) if f.startswith('update_leads_in_crm') and f.endswith('.json')]
    if not json_files:
        raise AirflowSkipException("Não foram encontrados updates de leads pendentes de sincronização.")

    kwargs['ti'].xcom_push(key='updated_lead_files', value=json_files)
    
def valida_representante(representante):
    url = 'http://api.example.com:5000/codgestores'
    response = requests.get(url)
    if response.status_code == 200:
        lista_cod_gestores = response.json()
        print('LISTA COD GESTORES')
        print(lista_cod_gestores)
        for cod_gestor in lista_cod_gestores:
            if str(representante) == cod_gestor:
                return representante
        
        return str(99)

    else:
        print('Nao foi possivel buscar os cod_gestores ' + response.text)

def update_on_crm(**kwargs):

    headers = {'Content-Type': 'application/json'}
    contador = 0
    for file in kwargs['ti'].xcom_pull(key='updated_lead_files'):
        with open(f'/data/integration/leads/processar/{file}', 'r') as f:
            content = f.readlines()
            for line in content:
                fields = {}
                file_data = json.loads(line)
                for key, value in file_data.items():
                    
                    fields[key.lower()] = value
                
                body = {
                    "duplicityValidation": "true",
                    'lead': {'customFields': []}
                }

                try:
                    body['lead']['name'] = fields['name']
                except KeyError as e:
                    raise e
                
                try:
                    body['lead']['address'] = fields['end_rua']
                except KeyError as e:
                    raise e
                try:
                    body['lead']['zipcode'] = str(fields['end_cep']).split('.')[0]
                except KeyError as e:
                    raise e
                try:
                    body['lead']['addressNumber'] = str(fields['end_numero']).split('.')[0]
                except KeyError as e:
                    body['lead']['addressNumber'] = 'SN'
                print('Numero enviado para a API:')
                print(body['lead']['addressNumber'])
                try:
                    body['lead']['addressComplement'] = fields['end_complemento']
                except KeyError as e:
                    body['lead']['addressComplement'] = ''
                try:
                    body['lead']['neighborhood'] = fields['end_bairro']
                    body['lead']['city'] = fields['end_cidade']
                    body['lead']['state'] = fields['end_estado']
                    body['lead']['cpfcnpj'] = str(fields['cnpj']).split('.')[0].zfill(14)
                    body['lead']['country'] = 'Brasil'
                    representante = str(int(fields['representante']))
                    cod_cliente = str(int(fields['cod_cliente']))
                    representante = valida_representante(representante)
                    inscricao_estadual = str(fields['inscricao_estadual']).strip()
                    analise_financeira = str(fields.get('analise_financeira', 'Vazio.'))
                    body['lead']['customFields'].append({'id': '_cod.gestores', "options": [{'value': representante}]})
                    body['lead']['customFields'].append({'id': '_cod.cliente', 'value': cod_cliente})
                    body['lead']['customFields'].append({'id': '_inscricaoestadual', 'value': "99" if inscricao_estadual == 'ISENTO' else inscricao_estadual})
                    body['lead']['customFields'].append({'id': '_analisefinanceira', 'value': analise_financeira})
                except KeyError as e:
                    raise KeyError(f'Chave ausente: {e.args[0]}')
                
                lead_id = int(fields['id'])

                url = f"http://api.example.com:5000/atualizalead/{lead_id}"

                for key, value in body.items():
                    if value == None:
                        print(f'Valor: {key} esta nulo')

                print(body)
                logger.log({'msg': 'Fazendo PUT da Lead no CRM API', 'dag_id': dag_id})

                response = requests.put(url, headers=headers, data=json.dumps(body))
                if response.status_code != 201:
                    logger.log({'msg': 'Nao foi possivel enviar o PUT para o CRM API', 'dag_id': dag_id})

                    raise requests.HTTPError(f'Não foi possivel atualizar o registro id {lead_id}: {response.text}')
                else:
                    contador += 1
                    print('Registro atualizado com sucesso!')
                    if contador == 10:
                        print('10 Leads enviadas para o CRM, interrompendo execucao')
                        break
            
            return f'{contador} registros atualizados com sucesso!'


def move_json_files(**kwargs):
    logger.log({'msg': 'Movendo os arquivos processados', 'dag_id': dag_id})

    ti = kwargs['ti']
    json_files = ti.xcom_pull(key='updated_lead_files')
    if json_files is None or not json_files:
        print("Nenhum arquivo JSON para mover.")
        return
    source_directory = '/data/integration/leads/processar'
    destination_directory = '/data/integration/leads/processados'
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
        task_id = 'task_atualiza_lead_no_crm',
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
