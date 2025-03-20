from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.oracle.hooks.oracle import OracleHook
import pendulum
import os
import requests
import json
import shutil
from logger import Logger

logger = Logger('file', '/opt/airflow/logs/execucao_tasks_airflow.log')

local_tz = pendulum.timezone("America/Sao_Paulo")

dag_id = 'job_insert_leads_in_crm'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

'''
    Esta DAG insere as leads novas no CRM.
'''

def log_inicio():
    logger.log({'msg': 'Iniciando execucao de criacao de leads no CRM', 'dag_id': dag_id})


'''
    Busca arquivos pendentes de integração, gerados pelo spark.
'''
def list_files(**kwargs):
    logger.log({'msg': 'Buscando json de insercao no CRM', 'dag_id': dag_id})
    source_directory = '/data/integration/leads/processar'
    json_files = [f for f in os.listdir(source_directory) if f.startswith('insert_leads_in_crm') and f.endswith('.json')]
    if not json_files:
        logger.log({'msg': 'Nenhum json para insercao no CRM foi gerado', 'dag_id': dag_id})
        raise AirflowSkipException("Não foram encontrados insert de leads pendentes de sincronização.")

    kwargs['ti'].xcom_push(key='insert_lead_files', value=json_files)


def valida_representante(representante):
    url = 'http://api.example.com:5000/codgestores'
    response = requests.get(url)
    if response.status_code == 200:
        lista_cod_gestores = response.json()
        for cod_gestor in lista_cod_gestores:
            if str(representante) == cod_gestor:
                return representante
        
        return 99

    else:
        print('Nao foi possivel buscar os cod_gestores ' + response.text)


def atualiza_id_crm_oracle(cnpj, id_crm):
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    row = (cnpj, id_crm)
    oracle_hook.callproc('PRC_GRAVA_ID_CRM_CLI', autocommit=True, parameters=row)


'''
    Monta o body da requisicao, descartando os campos que nao estao preenchidos.
'''
def create_body(file_data):
    body = {
        "duplicityValidation": "true",
        'lead': {}
    }

    fields = [
        ('name', 'NAME'),
        ('industry', 'INDUSTRY'),
        ('address', 'ADDRESS'),
        ('zipcode', 'ZIPCODE'),
        ('sdrEmail', 'EMAIL'),
        ('phone', 'PHONE'),
        ('addressNumber', 'NUMERO'),
        ('addressComplement', 'COMPLEMENT'),
        ('neighborhood', 'NEIGHBORHOOD'),
        ('city', 'CITY'),
        ('state', 'STATE'),
        ('cpfcnpj', 'CPFCNPJ')
    ]

    for lead_key, file_key in fields:
        try:
            body['lead'][lead_key] = (
                str(file_data[file_key]).zfill(14) if file_key == 'CPFCNPJ' and isinstance(file_data[file_key], (int, float))
                else str(file_data[file_key]) if isinstance(file_data[file_key], (int, float))
                else file_data[file_key])
        except KeyError as e:
            print(f"Chave ausente: {file_key}")

    representante = file_data['REPRESENTANTE']
    inscricao_estadual = file_data['INSCRICAO_ESTADUAL'].replace('.', '').replace('-', '')
    inscricao_estadual = '99' if inscricao_estadual == 'ISENTO' else inscricao_estadual
    print(inscricao_estadual)
    cod_gestor = valida_representante(representante)
    body['lead']['country'] = 'Brasil'
    body['lead']['customFields'] = [{
        'id': '_cod.gestores',
        'options': [
            {'value':  str(cod_gestor)}
        ]
    },
    {
        'id': '_cod.cliente', 'value': str(file_data['COD_CLIENTE'])
    },
    {
        'id': '_inscricaoestadual', 'value': inscricao_estadual
    }]

    logger.log({'msg': 'Body para INSERT no CRM API criado', 'dag_id': dag_id})

    return body

'''
    Busca contatos atrelados a nova lead integrada, se existir salva o contato em um json pronto para ser processado.
'''
def busca_contato(cnpj, lead_id):
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    sql = f'select NOME, EMAIL, TELEFONE, CARGO_DESCRICAO, CONTATO_PRINCIPAL from vw_crm_clientes_contatos where cliente_CNPJ = {cnpj}'
    contatos = oracle_hook.get_records(sql=sql)
    if contatos:
        for contato in contatos:
            print(contato)
            if contato[4] == 'S':
                mainContact = True
            else:
                mainContact = False
            email = contato[1]
            agora = datetime.now().strftime("%Y%m%d%H%M%S")
            contato_dict = {'name': contato[0], 'email': email, 'phone1': str(contato[2]), 'jobTitle': contato[3], 'mainContact': mainContact, 'leadId': str(lead_id)}
            with open(f'/data/integration/contacts/processar/insert_contact_crm_{agora}_{email.split("@")[0]}.json', 'w') as f:
                json.dump(contato_dict, f) 
'''
    Para cada linha dos arquivos jsons, cria o body e envia para o CRM (POST).
'''
def insert_on_crm(**kwargs):
    logger.log({'msg': 'Lendo arquivos para gerar a requisicao', 'dag_id': dag_id})

    headers = {'Content-Type': 'application/json'}
    url = f"http://api.example.com:5000/leads"
    contador = 0
    for file in kwargs['ti'].xcom_pull(key='insert_lead_files'):
        with open(f'/data/integration/leads/processar/{file}', 'r') as f:
            content = f.readlines()
            for line in content:
                file_data = json.loads(line)
                
                body = create_body(file_data)

                response = requests.post(url, headers=headers, data=json.dumps(body))

                if response.status_code != 200:
                    print(body)
                    logger.log({'msg': 'ERROR - Não foi possivel inserir a lead no CRM', 'dag_id': dag_id})
                    raise requests.HTTPError(f'Não foi possivel inserir a lead {file_data["NAME"]}: {response.text}')
                else:                    
                    print('Registro inserido com sucesso!')
                    lead_id = response.json()['lead_id']
                    cnpj = body['lead']['cpfcnpj']
                    atualiza_id_crm_oracle(cnpj, lead_id)
                    busca_contato(cnpj, lead_id)
                    logger.log({'msg': 'Lead inserido no CRM com sucesso', 'dag_id': dag_id})
                    contador += 1

            
            return f'{contador} registros inseridos com sucesso!'


'''
    Move os arquivos concluidos com sucesso para Processados.
'''
def move_json_files(**kwargs):
    ti = kwargs['ti']
    json_files = ti.xcom_pull(key='insert_lead_files')
    if json_files is None or not json_files:
        logger.log({'msg': 'Nenhum JSON para mover', 'dag_id': dag_id})
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
         schedule_interval='*/1 * * * *',
         max_active_runs=1,
         catchup=False) as dag:
    

    list_files = PythonOperator(
        task_id = 'task_lista_arquivos_pendentes',
        python_callable=list_files,
        provide_context=True
    )

    insert_on_crm = PythonOperator(
        task_id = 'task_insere_lead_no_crm',
        python_callable=insert_on_crm,
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

    log_start >> list_files >> insert_on_crm >> move_files
