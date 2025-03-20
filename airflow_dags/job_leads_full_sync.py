from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.hooks.oracle_hook import OracleHook
import pendulum
import requests
import json
import shutil
from logger import Logger
import logging
import os

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

dag_id = 'job_leads_full_sync'

now = datetime.now().strftime('%d_%m_%Y')

def log_missing_field(lead_id, field_name, lead_name=None, value=None, oracle_hook=None):
    logger_db = Logger('database', '/opt/airflow/logs/execucao_tasks_airflow.log')
    print(f'Lead ID {lead_id}: Campo {field_name} não encontrado ou nulo')
    logger_db.log({'lead_id': lead_id, 'lead_name': lead_name, 'field': field_name, 'value': value}, oracle_hook)


def log_missing_field_contato(lead_id, contato_name, field_name, value, oracle_hook):
    logger_db = Logger('database', '/opt/airflow/logs/execucao_tasks_airflow.log')
    print(f'Contato {contato_name} ID {lead_id}: Campo {field_name} não encontrado ou nulo')
    logger_db.log({'lead_id': lead_id, 'lead_name': contato_name, 'field': field_name, 'value': value}, oracle_hook)


def check_field(lead, field_name, nested_field=None, carga_inicial='S'):
    try:
        oracle_hook = OracleHook(oracle_conn_id='oracle_db')

        value = lead[field_name][nested_field] if nested_field else lead[field_name]
        if not value:
            log_missing_field(lead['id'], field_name if not nested_field else f"{field_name}.{nested_field}", lead['lead'], 'Campo vazio', oracle_hook)
        return bool(value)
    except KeyError:
        if carga_inicial:
            try:
                log_missing_field(lead['id'],  field_name if not nested_field else f"{field_name}.{nested_field}", lead['lead'], 'Campo ausente', oracle_hook)
            except Exception as e:
                print(f'Erro ao validar o campo {field_name} da lead {lead["lead"]}')
                print(e)
        return False
   


def valida_lead_entrada(lead, CARGA_INICIAL='S'):
    etapa = lead.get('stage')
    if etapa not in ('Desenvolvimento/Orçamento', 'Ativação', 'Retenção/Expansão'):
        # Integrar apenas o id, lead Ok
        return True
    else:
        oracle_hook = OracleHook(oracle_conn_id='oracle_db')
        try:
            representante, inscricao_estadual, analise_financeira = busca_custom_fields(lead['id'])
            lead['representante'] = representante
            lead['inscricao_estadual'] = inscricao_estadual
            lead['analise_financeira'] = analise_financeira

            if not (representante) and CARGA_INICIAL == 'S':
                log_missing_field(lead['id'], 'REPRESENTANTE', lead['lead'], 'Campo ausente', oracle_hook)
                return False
            

        except Exception as e: 
            if CARGA_INICIAL == 'S':
                campo_ausente = e.args[0]
                print('Campo ausente = ' + campo_ausente)
                log_missing_field(lead['id'], campo_ausente, lead['lead'], 'Campo ausente', oracle_hook)
                print(e)
            return False

        fields_to_check = [
            ('id', None),
            ('lead', None),
            ('cnpj', None),
            ('industry', 'value'),
            ('source', 'value'),
            ('street', None),
            ('cep', None),
            ('number', None),
            ('district', None),
            ('city', None),
            ('state', None)
        ]

        for field, nested_field in fields_to_check:
            if not check_field(lead, field, nested_field, CARGA_INICIAL):
                return False

        return True



def log_inicio():
    logger.log({'msg': 'Iniciando execucao sincronizacao full das leads', 'dag_id': dag_id})
    

'''
    Faz um GET no CRM API buscando TODAS as leads do CRM.
'''
def get_all_leads():
    url = 'http://api.example.com:5000/leads'
    response = requests.get(url, timeout=80)
    if response.status_code == 200:
        leads_json = response.json()
        with open(f'/data/integration/leads/processar/full_sync_leads_{now}.json', 'w') as f:
            json.dump(leads_json, f)
    
    else:
        raise requests.exceptions.HTTPError(f'Erro ao buscar as leads - Status code: {response.status_code}')

def busca_custom_fields(lead_id):
    url_representante = f'http://api.example.com:5000/representante/{lead_id}'
    headers = {'Content-Type': 'application/json'}
    repr_response = requests.get(url_representante, headers=headers)
    if repr_response.status_code == 200:
        representante = repr_response.json()
    else:
        raise(ValueError('Erro ao buscar o representante: ' + repr_response.text))

    url_inscrestadual = f'http://api.example.com:5000/inscricaoestadual/{lead_id}'
    inscrestadual_response = requests.get(url_inscrestadual, headers=headers)
    if inscrestadual_response.status_code == 200:
        inscricao_estadual = inscrestadual_response.json()


    url_analisefinanceira = f'http://api.example.com:5000/analisefinanceira/{lead_id}'
    ananlisefinanceira_response = requests.get(url_analisefinanceira, headers=headers)
    analise_financeira = ''
    if ananlisefinanceira_response.status_code == 200:
        analise_financeira = ananlisefinanceira_response.json()

    return representante, inscricao_estadual, analise_financeira


def valida_contato_entrada(contato, lead_cnpj, carga_inicial='S'):
    """
    Valida os campos obrigatórios de um contato associado a um lead.

    Args:
        contato (dict): Dicionário contendo os dados do contato.
        lead_cnpj (str): CNPJ associado ao lead.

    Returns:
        bool: True se o contato for válido, False caso contrário.
    """
    required_fields = ['id', 'name', 'email', 'phone1', 'jobTitle']
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    # Verifica se os campos obrigatórios estão presentes e preenchidos
    for field in required_fields:
        if not contato.get(field):
            if carga_inicial == 'S':
                print('CARGA INICIAL = S')
                log_missing_field_contato(lead_cnpj, f"Contato: {contato['name']}", field, 'Valor ausente', oracle_hook)

            return False
    
    # Contato é válido, adiciona CNPJ ao contato (apenas para referência)
    contato['lead_cnpj'] = lead_cnpj
    return True

def get_contatos(lead_id):
    headers = {'Content-Type': 'application/json'}
    url = f'http://api.example.com:5000/buscacontato/{lead_id}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        contatos = response.json()
        return contatos
    else:
        raise requests.exceptions.HTTPError(f'Erro ao buscar os contatos - Status code: {response.status_code}')

def processa_e_valida_contatos(lead_id, CARGA_INICIAL='S'):

    contacts = get_contatos(lead_id)['value']

    if contacts:
        contatos_integrar = []
        for contact in contacts:
            contact_ok = valida_contato_entrada(contact, lead_id, CARGA_INICIAL)
            if not contact_ok:
                if CARGA_INICIAL == 'S':
                    print('Erro ao validar o contato: ' + contact['name'])
                else:
                    #raise ValueError('Erro ao validar o contato')
                    print('Erro no contato: ' + contact['name'])
            else:
                contatos_integrar.append(contact)
    
        if CARGA_INICIAL == 'S':
            print('Todos os contatos foram validados com sucesso.')
        else:
            print('Inserindo contatos no Oracle')
            insert_contact_to_oracle(contatos_integrar)


def insert_contact_to_oracle(contatos):
    rows = []
    #   Busca o COD_CLIENTE do lead
    query = 'SELECT COD_CLIENTE FROM clientes WHERE id_crm = :lead_id'
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')

    for contato in contatos:
        cod_cliente = oracle_hook.get_first(query, parameters={'lead_id': contato['leadId']})[0]
        main_contact = contato['mainContact']
        if main_contact == 'true':
            main_contact = 'S'
        else:
            main_contact = 'N'

        row = (
            contato['id'],
            contato['name'],
            contato['email'],
            contato['phone1'][:13],
            contato['jobTitle'][:30],
            cod_cliente,
            main_contact
        )
        print(row)
        rows.append(row)
    
    rows = tuple(rows)

    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    oracle_hook.insert_rows(
        table='contatos',
        rows=rows,
        target_fields=['id', 'nome', 'email', 'telefone', 'cargo', 'cod_cliente', 'contato_principal'],
        commit_every=200,
        replace=False
    )
    print('Contatos inseridos com sucesso')


def atualiza_id_crm_oracle(cnpj, id_crm):
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    row = (cnpj, id_crm)
    oracle_hook.callproc('PRC_GRAVA_ID_CRM_CLI', autocommit=True, parameters=row)    

'''
    Insere as leads na etapa de Desenvolvimento/Orçamento no banco
'''
def insert_leads_to_oracle():
    with open(f'/data/integration/leads/processar/full_sync_leads_{now}.json', 'r') as f:
        leads_data = json.load(f)

    # Format data for Oracle insertion
    CARGA_INICIAL = 'N'
    rows = []
    for lead in leads_data:
        
        
        if lead['stage'] in ('Desenvolvimento/Orçamento', 'Ativação', 'Retenção/Expansão'):
            lead_ok = valida_lead_entrada(lead, CARGA_INICIAL)

            if lead_ok:
                representante, inscricao_estadual, analise_financeira = busca_custom_fields(lead['id'])

                
                row = (
                    lead['id'],
                    lead['lead'][:40], 
                    lead['stage'],
                    lead['cnpj'],
                    lead['industry']['value'],
                    lead['source']['value'],
                    lead['phone1'],
                    lead['street'].split()[0],
                    lead['cep'].replace('-', '').replace('.', ''),
                    0 if not isinstance(lead.get('number'), int) else lead.get('number', 0),
                    lead['complement'],
                    lead['district'],
                    lead['city'],
                    lead['state'],
                    representante,
                    'ISENTO' if str(inscricao_estadual) == '99' else str(inscricao_estadual),
                    analise_financeira
                )

                rows.append(row)
            else:
                print('Erro ao validar a lead: ' + str(lead['id']))
            
        else:
            if CARGA_INICIAL == 'N':
                print('Skipada')
                #atualiza_id_crm_oracle(lead['cnpj'], lead['id'])

    
    print(f'Numero de leads a serem integradas: {len(rows)}')
    rows = tuple(rows)
    contador = 0
    for row in rows:
        print(f'Lead {contador}: {row}')
        contador += 1

    if CARGA_INICIAL == 'S':
        print('Carga inicial, dados validaddos')
        print('Iniciando validacao dos contatos')
        for lead in leads_data:
            if lead['id'] in [row[0] for row in rows]:
                processa_e_valida_contatos(lead['id'], CARGA_INICIAL)
    else: 
        # Oracle Hook
        oracle_hook = OracleHook(oracle_conn_id='oracle_db')
        for row in rows:
            print('Inserindo a lead: ' + str(row))
            oracle_hook.insert_rows(
                table='leads',
                rows=[row],
                target_fields=['id', 'name', 'stage', 'cnpj', 'mercado', 'origem', 'telefone', 'end_rua', 'end_cep', 'end_numero', 'end_complemento', 'end_bairro', 'end_cidade', 'end_estado', 'representante', 'inscricao_estadual', 'analise_financeira'],  
                commit_every=1,
                replace=False
            )

        print('Iniciando validacao dos contatos')

        for lead in leads_data:
            if lead['id'] in [row[0] for row in rows]:
                print(lead['id'])
                processa_e_valida_contatos(lead['id'], CARGA_INICIAL)


'''
    Move os arquivos de sincronizacao concluidos para Processados.
'''
def move_json_file():
    source = f'/data/integration/leads/processar/full_sync_leads_{now}.json'
    destination = f'/data/integration/leads/processados/full_sync_leads_{now}.json'

    # Move o arquivo
    shutil.move(source, destination)

with DAG(dag_id,
    schedule_interval=None,
    default_args=default_args,
    max_active_runs=1,
    catchup=False
) as dag:
    
    request_leads = PythonOperator(
        task_id='request_leads',
        python_callable=get_all_leads,
    )

    truncate_table_leads = OracleOperator(
        task_id='truncate_table_leads',
        oracle_conn_id='oracle_db',
        sql='TRUNCATE TABLE leads',
    )

    truncate_table_contatos = OracleOperator(
        task_id='truncate_table_contatos',
        oracle_conn_id='oracle_db',
        sql='TRUNCATE TABLE contatos',
    )

    insert_leads = PythonOperator(
        task_id='insert_leads',
        python_callable=insert_leads_to_oracle,
    )

    move_file = PythonOperator(
        task_id='move_file',
        python_callable=move_json_file,
    )

    log_start = PythonOperator(
        task_id='start_processes',
        python_callable=log_inicio
    )

    log_start >> request_leads >> truncate_table_leads >> truncate_table_contatos >> insert_leads >> move_file
