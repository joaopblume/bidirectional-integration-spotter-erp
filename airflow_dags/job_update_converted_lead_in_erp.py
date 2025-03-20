from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
import pendulum
import json
import shutil
import os
import requests
from logger import Logger

logger = Logger('file', '/opt/airflow/logs/execucao_tasks_airflow.log')

local_tz = pendulum.timezone("America/Sao_Paulo")

dag_id = 'job_update_leads_convertidas'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}


def log_inicio():
    logger.log({'msg': 'Iniciando conversao de leads', 'dag_id': dag_id})



'''
    Esta função tem o objetivo de validar os jsons de atualização de etapa pendentes.
    Lista todos os jsons na fila e salva no xcom seus nomes.
    Salva o cnpj de cada JSON no xcom.
'''
def get_converted_leads_json_names(**kwargs):
    logger.log({'msg': 'Buscando leads convertidas pendentes de sincronizacao', 'dag_id': dag_id})
    source_directory = '/data/integration/leads/processar'
    json_files = [f for f in os.listdir(source_directory) if f.endswith('.json') and 'converted_lead' in f]
    lead_cnpjs = []
    if json_files:
        print(f'{len(json_files)} arquivos encontrados para sincronizacao')
        for json_file in json_files:
            file_path = os.path.join(source_directory, json_file)
            with open(file_path, 'r') as file:
                data = json.load(file)
                lead_cnpjs.append(data['Lead']['CpfCnpj'])
        kwargs['ti'].xcom_push(key='converted_lead_files', value=json_files)
        kwargs['ti'].xcom_push(key='lead_cnpjs', value=lead_cnpjs)
    else:
        logger.log({'msg': 'Não há arquivos para processar', 'dag_id': dag_id})

        raise AirflowSkipException('Não há arquivos json para processar')


def get_lead_id_by_cnpj(cnpj):
    print(f'CNPJ RECEBIDO: {cnpj}')
    url = f"http://api.example.com:5000/leadid/{cnpj}"
    response = requests.get(url)
    if response.status_code != 200:
        raise requests.HTTPError(f'Não foi possivel encontrar o lead com cnpj {cnpj}: {response.json()}')
    else:
        lead_id = response.json()['id']
        print(f'Lead ID: {lead_id}')
        return lead_id


def extract_lead_info(lead_data, lead_id, new_stage):
    lead = {}
    analise_financeira = None
    try:
        for custom_field in lead_data['Lead']['CustomFields']:
            if custom_field['id'] == '_inscricaoestadual':
                inscricao_estadual = custom_field['value']
            if custom_field['id'] == '_cod.gestores':
                representante_id = custom_field['value']
            if custom_field['id'] == '_analisefinanceira':
                analise_financeira = custom_field['value']

        # Completa as informações de da lead com os dados do lead_add_info
        lead_add_info = get_lead_add_info(lead_id)

        lead['id'] = lead_id
        print(f'Processando lead: {lead_id}')

        if lead_id != 12345678:  # Exemplo genérico de ID
            lead['cnpj'] = lead_data['Lead']['CpfCnpj']
            print(f'Caputado CNPJ: {lead_data["Lead"]["CpfCnpj"]}')
            lead['name'] = lead_data['Lead']['Company'][:40]
            lead['stage'] = new_stage
            lead['mercado'] = lead_data['Lead']['Industry']['value']
            lead['origem'] = lead_data['Lead']['Origin']['value']
            lead['telefone'] = lead_add_info['telefone']

            lead['end_rua'] = lead_data['Lead']['Address']['Address_Maps'].split(',')[0].split('-')[0][:50]
            lead['end_cep'] = lead_data['Lead']['Address']['ZipCode'].replace('-', '').replace('.', '')
            lead['end_numero'] = 0 if not isinstance(lead_add_info.get('numero'), int) else lead_add_info.get('numero', 0)
            lead['end_complemento'] = lead_add_info['complemento']
            lead['end_bairro'] = lead_add_info['bairro'] 
            lead['end_cidade'] = lead_add_info['cidade']
            lead['end_estado'] = lead_add_info['estado']
            lead['representante'] = representante_id
            try:    
                lead['inscricao_estadual'] = 99 if inscricao_estadual in ('99', '0') else inscricao_estadual
            except UnboundLocalError:
                lead['inscricao_estadual'] = '99'
            lead['analise_financeira'] = analise_financeira
            return lead
        else:
            print('a')
    except Exception as e:
        print('Lead Add Info:')
        print(lead_add_info)
        print('Lead Data:')
        print(lead_data)
        raise e

    
'''
    Le a lista de arquivos pendente de sincronizao.
'''
def process_and_update_leads(**kwargs):
    ti = kwargs['ti']
    json_files = ti.xcom_pull(task_ids='get_converted_leads_json_names', key='converted_lead_files')
    lead_cnpjs = ti.xcom_pull(task_ids='get_converted_leads_json_names', key='lead_cnpjs')
    
    integrar_contatos = []
    contador = 0
    for json_file, lead_cnpj in zip(json_files, lead_cnpjs):
        if 'EXAMPLE_COMPANY' in json_file:
            continue
        print('-----')
        print(f'Processando arquivo: {json_file}')
        print(f'Lead CNPJ: {lead_cnpj}')
        file_path = os.path.join('/data/integration/leads/processar', json_file)
        with open(file_path, 'r') as f:
            lead_data = json.load(f)
        new_stage = lead_data['Lead']['Stage']
        logger.log({'msg': 'Processando leads convertidas', 'dag_id': dag_id})

        if new_stage == 'Desenvolvimento/Orçamento':
            print(f'CNPJ')
            print(lead_cnpj)
            lead_id = get_lead_id_by_cnpj(lead_cnpj)
            lead = extract_lead_info(lead_data, lead_id, new_stage)
            insert_erp_lead(lead)
            integrar_contatos.append((lead_id, json_file))
            print('Registrou o contato na lista de integrados.')
        elif new_stage == 'Ativação':
            lead_id = get_lead_id_by_cnpj(lead_cnpj)
            update_stage_in_erp(lead_id, new_stage)

        else:
            print(f'Lead atualizada ainda não está integrada no CRM. Etapa do lead: {new_stage}')

        
        if contador == 1000:
            print('1000 leads converted processadas')
            break

        contador += 1
    ti.xcom_push(key='integrados', value=(integrar_contatos))


def get_lead_add_info(lead_id):
    url = f'http://api.example.com:5000/leadaddinfo/{lead_id}'

    response = requests.get(url)

    try:
        infos = response.json()
        return infos
    except Exception as e:
        raise requests.HTTPError(f'Não foi possivel obter informações adicionais do lead {lead_id}: {response.text} {e}')
    

def update_stage_in_erp(lead_id, new_stage):
    logger.log({'msg': 'Atualizando etapa da lead no ERP', 'dag_id': dag_id})
    sql = "update leads set stage = :1 where id = :2"
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    row = (new_stage, lead_id)
    oracle_hook.run(sql, parameters=row)

'''
    Envia um put request na api com os campos que serão atualizados no body.
'''
def update_crm_cliente(lead_id, cod_cliente):
    logger.log({'msg': 'Atualizando o representante da lead no CRM', 'dag_id': dag_id})

    url = f"http://api.example.com:5000/leads"
    headers = {'Content-Type': 'application/json'}
    body = {"Id": lead_id, "Fields": [{"field_name": "_cod.cliente", "field_value": cod_cliente}]}

    response = requests.put(url, headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        raise requests.HTTPError(f'Não foi possivel atualizar o registro id {lead_id}: {response.text}')

def insert_erp_lead(lead: dict):
    logger.log({'msg': 'Inserindo nova lead no ERP', 'dag_id': dag_id})

    sql = """insert into leads (
                     id,
                     cnpj,
                     name,
                     stage,
                     mercado,
                     origem,
                     telefone,
                     end_rua,
                     end_cep,   
                     end_numero,
                     end_complemento,
                     end_bairro,
                     end_cidade,
                     end_estado,
                     representante,
                     inscricao_estadual,
                     analise_financeira
            )
        values
        (:id, :cnpj, :name, :stage, :mercado, :origem, :telefone, :end_rua, :end_cep, :end_numero, :end_complemento, :end_bairro, :end_cidade, :end_estado, :representante, :inscricao_estadual, :analise_financeira)"""
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    info = [value for _key, value in lead.items()]
    print([v for v in lead.items()])
    try:
        oracle_hook.run(sql, parameters=info)
    except Exception as e:
        raise e



'''
    Função que move os jsons processados para o diretorio de processados, cria um json de contatos a serem integrados.
'''
def move_json_files(**kwargs):
    logger.log({'msg': 'Movendo leads processadas para PROCESSADOS', 'dag_id': dag_id})

    ti = kwargs['ti']
    json_files = ti.xcom_pull(task_ids='get_converted_leads_json_names', key='converted_lead_files')
    if json_files is None or not json_files:
        print("Nenhum arquivo JSON para mover.")
        return
    
    source_directory = '/data/integration/leads/processar'
    destination_directory = '/data/integration/leads/processados'
    contacts_directory = '/data/integration/contacts/processar'

    integrados = ti.xcom_pull(task_ids='process_leads', key='integrados')
    print('Valor de INTEGRADOS no move_json_files: ')
    print(integrados)
    for json_file in json_files:
        source = os.path.join(source_directory, json_file)

        # abre o arquivo source, valida se o lead_id é o mesmo no xcom na key "integrados"
        with open(source, 'r') as f:
            lead_data = json.load(f)
            lead_id = lead_data['Lead']['Id']
            print(f'Printando lead_id: {lead_id}')
            lead_cnpj = lead_data['Lead']['CpfCnpj']

            if integrados:
                # Se o lead_id estiver na lista de integrados, cria um arquivo de contato na fila de processamento
                if [lead_id, json_file] in integrados:
                    print(f'Lead_id {lead_id} encontrado na lista de integrados')
                    for contato in lead_data['Lead']['Contact']:
                        contact_dict = contato
                        contact_dict['lead_cnpj'] = lead_cnpj
                        with open(os.path.join(contacts_directory, f'contato_update_erp_{lead_id}_{contact_dict["Id"]}.json'), 'w') as f:
                            json.dump(contact_dict, f)

                else:
                    print(f'O lead_id {lead_id} nao foi encontrado na lista de leads integradas {integrados}')
        # Move o json da lead para o diretorio de processados
        destination = os.path.join(destination_directory, json_file)
        shutil.move(source, destination)
        print(f"Arquivo {json_file} movido para {destination}")


# Definição do fluxo:
with DAG(dag_id,
            schedule_interval='*/10 * * * *',
            default_args=default_args,
            max_active_runs=1,
            catchup=False) as dag:
    
    get_converted_leads_json_names = PythonOperator(
        task_id='get_converted_leads_json_names',
        python_callable=get_converted_leads_json_names
    )
    
    process_leads = PythonOperator(
        task_id='process_leads',
        python_callable=process_and_update_leads
    )

    trigger_update_contatos = TriggerDagRunOperator(
        task_id='trigger_update_contatos',
        trigger_dag_id='job_update_contact_in_erp',
    )

    move_files = PythonOperator(
        task_id='move_files',
        python_callable=move_json_files,
        trigger_rule=TriggerRule.ALL_SUCCESS,  
    )

    log_start = PythonOperator(
        task_id='start_processes',
        python_callable=log_inicio
    )

    log_start >> get_converted_leads_json_names >> process_leads >> move_files >> trigger_update_contatos
