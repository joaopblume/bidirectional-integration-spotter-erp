from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.exceptions import AirflowSkipException
import pendulum
import requests
import json
import shutil
from logger import Logger
import os
import logging
import unicodedata


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

dag_id = 'job_descarta_lead'

now = datetime.now().strftime('%d_%m_%Y')


def log_inicio():
    logger.log({'msg': 'Iniciando delecao de leads descartadas', 'dag_id': dag_id})


def busca_etapa_atual(lead_cnpj):
    url = 'http://api.example.com:5000/leadstage/' + str(lead_cnpj)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print(response.json())
        return response.json()
    else:
        raise KeyError('Não foi possivel buscar a etapa da lead') 
    

def reinicio_de_ciclo():
    # Busca o ID das leads na tabela de leads
    stages_order = ('Descartado', 'Novos leads', 'Pesquisa (filtro 1)', 'Prospecção (filtro 2)', 'Passagem de bastão', 'Desenvolvimento/Orçamento', 'Ativação', 'Retenção/Expansão')
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    select_statement = 'SELECT id, stage, cnpj FROM leads'
    leads = oracle_hook.get_records(select_statement)
    leads_atualizadas = 0
    for lead in leads:
        print(lead)
        stage = lead[1]
        cnpj = lead[2]
        etapa_atual = busca_etapa_atual(cnpj).replace('\xa0', ' ')
        lead_id = lead[0]
        print('Etapa atual: ' + str(etapa_atual))
        print('Etapa na tabela: ' + str(stage))
        try:
            if etapa_atual is None:
                oracle_hook.run('DELETE FROM leads WHERE id = :id', parameters={'id': lead_id})
                logger.log({'msg': f'Lead {lead_id} foi DESCARTADA, pois não foi encontrada no CRM', 'dag_id': dag_id})
                leads_atualizadas += 1
            # Se o index da etapa atual for menor que o index da etapa atual, reinicia a etapa
            elif stages_order.index(etapa_atual) < stages_order.index(stage):
                oracle_hook.run('DELETE FROM leads WHERE id = :id', parameters={'id': lead_id})
                logger.log({'msg': f'Lead {lead_id} reiniciada para a etapa {etapa_atual}', 'dag_id': dag_id})
                leads_atualizadas += 1
            else:
                logger.log({'msg': f'Lead {lead_id} ja esta na etapa {etapa_atual}', 'dag_id': dag_id})

        except ValueError:
            print(f'Lista: {stages_order}')
            raise ValueError('Etapa nao encontrada na lista')
    if leads_atualizadas > 0:
        print(f'{leads_atualizadas} Leads foram reiniciadas.')
    else:
        raise(AirflowSkipException('Nenhuma lead foi reiniciada.'))


def move_json_file(file_name):
    destination = f'/data/leads/processados/{file_name}'
    source = f'/data/leads/processar/{file_name}'
    shutil.move(source, destination)

def get_lost_leads():
    source_directory = '/data/leads/processar'
    for file in os.listdir(source_directory):
        if file.endswith('.json') and file.startswith('lead_descartada'):
            move_json_file(file)



with DAG(dag_id,
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    max_active_runs=1,
    catchup=False
) as dag:
    

    log_start = PythonOperator(
        task_id='start_processes',
        python_callable=log_inicio
    )

    descartadas = PythonOperator(
        task_id='start',
        python_callable=get_lost_leads
    )


    ciclo_reiniciado = PythonOperator(
        task_id='reinicio_de_ciclo',
        python_callable=reinicio_de_ciclo
    )

    log_start  >> ciclo_reiniciado >> descartadas
