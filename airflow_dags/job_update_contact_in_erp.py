from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
import pendulum
import json
from airflow.exceptions import AirflowSkipException
import os
from logger import Logger
import requests
import shutil


logger = Logger('file', '/opt/airflow/logs/execucao_tasks_airflow.log')
logger_db = Logger('database', '/opt/airflow/logs/execucao_tasks_airflow.log')
local_tz = pendulum.timezone("America/Sao_Paulo")

dag_id = 'job_update_contact_in_erp'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}      

'''
Esta DAG possui o seguinte fluxo:
1 -> Lê os jsons do lote de leads atualizadas.
2 -> Atualiza o contato dos jsons selecionados na etapa 1.
'''

def log_inicio():
    logger.log({'msg': 'Iniciando atualizacao de contatos no erp', 'dag_id': dag_id})

def get_contact_files(**kwargs):
    logger.log({'msg': 'Buscando contatos pendentes de sincronizacao', 'dag_id': dag_id})
    source_directory = '/data/integration/contacts/processar'
    json_files = [f for f in os.listdir(source_directory) if f.endswith('.json') and 'contato_update_erp' in f]
    if json_files:
        print(f'{len(json_files)} arquivos encontrados para sincronizacao')
        kwargs['ti'].xcom_push(key='contact_files', value=json_files)
        print(kwargs)
    else:
        logger.log({'msg': 'Não há arquivos para processar', 'dag_id': dag_id})

        raise AirflowSkipException('Não há arquivos json para processar')

def main_contact(lead_id, contact_id):
    url = f'http://api.example.com:5000/maincontact/{lead_id}/{contact_id}'
    response = requests.get(url)
    return response.json()

        

def execute_sql(contato, op):
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    fone = contato.get('Phone', None)
    if fone:
        fone = str(fone)[:13]
    row = (
        contato['Name'],
        contato['Email'],
        #contato['Phone'],
        fone,
        contato['Position'][:30],
        contato['cod_cliente'],
        contato['contato_principal'],
        contato['Id']
    )

    if op == 'I':
        insert = 'INSERT INTO CONTATOS (NOME, EMAIL, TELEFONE, CARGO, COD_CLIENTE, CONTATO_PRINCIPAL, ID) values (:1, :2, :3, :4, :5, :6, :7)'
        print(insert)
        print(row)
        oracle_hook.run(insert, parameters=row)

    elif op == 'U':
        update = 'UPDATE CONTATOS SET NOME = :1, EMAIL = :2, TELEFONE = :3, CARGO = :4, COD_CLIENTE = :5, CONTATO_PRINCIPAL = :6 WHERE ID = :7'
        print(update)
        print(row)
        oracle_hook.run(update, parameters=row)


def insere_contato_erp(**kwargs):
    files_list = kwargs['ti'].xcom_pull(key='contact_files', task_ids='list_contact_files')
    contatos = []
    # Cria a lista de jsons lidos no diretório de processamento
    logger.log({'msg': 'Extraindo dados dos contatos', 'dag_id': dag_id})

    source_directory = '/data/integration/contacts/processar'
    for json_name in files_list:
        file_path = os.path.join(source_directory, json_name)


        with open(file_path, 'r') as file:
            contato = json.load(file)

            contatos.append(contato)

    contatos_com_problema = []
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    for contato in contatos:
        try:
            is_main_contact = main_contact(contato['LeadId'], contato['Id'])
            contato['contato_principal'] = is_main_contact
            email = contato['Email']
        except Exception as e:
            contatos_com_problema.append(contato)

            logger_db.log({'lead_id': str(contato.get('LeadId', 'N_IDENTIFICADA')), 'lead_name': f"Contato: {contato.get('Name', 'SEM_NOME')}", 'field': 'Email', 'value': 'Email inexistente'}, oracle_hook)
            print(f'Contato {contato["Name"]} sem email ou contato inexistente')

            continue


        busca_contato_sql = f"SELECT COUNT(*) from contatos where email = '{email}'"    
        oracle_hook = OracleHook(oracle_conn_id='oracle_db')
        cont = oracle_hook.get_first(sql=busca_contato_sql)
            
        
        lead_cnpj = contato['lead_cnpj']
        busca_cod_cliente = f"SELECT COD_CLIENTE from vw_crm_clientes where CNPJ = '{lead_cnpj}'"
        cod_cliente = oracle_hook.get_first(sql=busca_cod_cliente)
        if cod_cliente:
            cod_cliente = cod_cliente[0]
            contato['cod_cliente'] = cod_cliente
        else:
            contatos_com_problema.append(contato)
            logger_db.log({'lead_id': str(contato.get('LeadId', 'N_IDENTIFICADA')), 'lead_name': f"Contato: {contato.get('Name', 'SEM_NOME')}", 'field': 'LeadCNPJ', 'value': 'CNPJ da Lead deste contato nao encontrado'}, oracle_hook)
            print(f'Contato {contato["Name"]} SEM CNPJ da lead')
            continue
        
        if cont:
            cont = cont[0]

            if cont == 1:
                # Ja existe no ERP, atualizar
                try:
                    execute_sql(contato, 'U')
                except Exception as e:
                    contatos_com_problema.append(contato)
                    logger_db.log({'lead_id': str(contato.get('LeadId', 'N_IDENTIFICADA')), 'lead_name': f"Contato: {contato.get('Name', 'SEM_NOME')}", 'field': '???', 'value': f'Nao foi possivel atualizar no banco de dados: {e[:200]}'}, oracle_hook)
                    print(f'Não conseguiu atualizar no banco de dados o contato {contato["Name"]}')

            elif cont == 0:
                # nao existe no ERP, criar
                try:
                    execute_sql(contato, 'I')
                except Exception as e:
                    contatos_com_problema.append(contato)
                    logger_db.log({'lead_id': contato.get('LeadId', 'N_IDENTIFICADA'), 'lead_name': f"Contato: {contato.get('Name', 'SEM_NOME')}", 'field': '???', 'value': f'Nao foi possivel inserir no banco de dados: {e}'}, oracle_hook)
                    print(f'Não conseguiu inserir no banco de dados o contato {contato["Name"]}')

    kwargs['ti'].xcom_push(key='contatos_com_problema', value=contatos_com_problema)

def move_processed_files(**kwargs):
    files = kwargs['ti'].xcom_pull(key='contact_files', task_ids='list_contact_files')
    source_dir = '/data/integration/contacts/processar/'
    dest_dir = '/data/integration/contacts/processados/'
    falhas_dir = '/data/integration/contacts/falhas/'
    contatos_com_problema = kwargs['ti'].xcom_pull(key='contatos_com_problema', task_ids='insere_contato_erp')
    print('Contatos com problema:')
    print(contatos_com_problema)

    for json_name in files:
        file_path = os.path.join(source_dir, json_name)


        with open(file_path, 'r') as file:
            contato = json.load(file)


            if contato['Id'] in [contato['Id'] for contato in contatos_com_problema]:
                source_path = os.path.join(source_dir, json_name)
                dest_path = os.path.join(falhas_dir, json_name)
                shutil.move(source_path, dest_path)
            else:

                source_path = os.path.join(source_dir, json_name)
                dest_path = os.path.join(dest_dir, json_name)
                shutil.move(source_path, dest_path)

with DAG(dag_id,
         schedule_interval='*/10 * * * *',
         default_args=default_args,
         max_active_runs=1,
         catchup=False) as dag:


        list_contact_files = PythonOperator(
            task_id='list_contact_files',
            python_callable=get_contact_files,
            provide_context=True
        )


        insere_contato_erp = PythonOperator(
            task_id='insere_contato_erp',
            python_callable=insere_contato_erp,
        )
        
        log_start = PythonOperator(
            task_id='start_processes',
            python_callable=log_inicio
        )

        move_processed_files = PythonOperator(
            task_id='move_processed_files',
            python_callable=move_processed_files
        )

log_start >> list_contact_files >> insere_contato_erp >> move_processed_files
