from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import json
from datetime import datetime
import os

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}

def fetch_leads_from_api():
    url = 'http://api.example.com:5000/leads'
    response = requests.get(url, timeout=80)
    if response.status_code == 200:
        now = datetime.now().strftime('%Y%m%d')

        leads_json = response.json()
        with open(f'/app/data/leads/processar/validade_full_{now}.json', 'w') as f:
            json.dump(leads_json, f)
    
    else:
        raise requests.exceptions.HTTPError(f'Erro ao buscar as leads - Status code: {response.status_code}')


def leads_in_dict():
    now = datetime.now().strftime('%Y%m%d')
    with open(f'/app/data/leads/processar/validade_full_{now}.json', 'r') as f:
        leads = json.load(f)
        return leads
    

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

def clean_string(value):
    if isinstance(value, str):
        return value.replace('.', '').replace('-', '')
    return value  # Retorna None ou o valor original, se não for string


def insert_leads_to_gtt():
    leads = leads_in_dict()
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    conn = oracle_hook.get_conn()
    cursor = conn.cursor()

    # Insere as leads na GTT
    for lead in leads:
        if lead['stage'] in ('Desenvolvimento/Orçamento', 'Ativação', 'Retenção/Expansão') and lead['id'] != 12345678:
            representante, inscricao_estadual, analise_financeira = busca_custom_fields(lead['id'])

            INSERT_GTT = """
                INSERT INTO LEADS_GTT (ID, NAME, STAGE, CNPJ, MERCADO, ORIGEM, TELEFONE, END_RUA,
                    END_CEP, END_NUMERO, END_COMPLEMENTO, END_BAIRRO, END_CIDADE, END_ESTADO,
                    REPRESENTANTE, INSCRICAO_ESTADUAL, ANALISE_FINANCEIRA)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17)
            """

            values = (
                lead['id'], lead['lead'], lead['stage'], lead['cnpj'], lead['industry']['value'], lead['source']['value'],
                clean_string(lead.get('phone1', None)), lead['street'], clean_string(lead.get('cep', None)), clean_string(lead.get('number', None)) if lead['number'] != 'SN' else 0, lead['complement'],
                lead['district'], lead['city'], lead['state'],
                representante, inscricao_estadual if inscricao_estadual != 'ISENTO' else 99, analise_financeira
            )

            print(INSERT_GTT)
            print(values)
            cursor.execute(INSERT_GTT, values)

    differences = identify_differences(cursor)
    with open(f'/app/data/leads/processar/validade_full_diferencas.json', 'w') as f:
        json.dump(differences, f)

def identify_differences(cursor, **kwargs):
    # Identifica as diferenças entre LEADS_GTT e LEADS
    cursor.execute("""
        SELECT src.ID, src.NAME, src.STAGE, src.CNPJ, src.MERCADO, src.ORIGEM, src.TELEFONE, src.END_RUA,
               src.END_CEP, src.END_NUMERO, src.END_COMPLEMENTO, src.END_BAIRRO, src.END_CIDADE, src.END_ESTADO,
               src.REPRESENTANTE, src.INSCRICAO_ESTADUAL,
               src.ANALISE_FINANCEIRA, tgt.ID AS tgt_id
        FROM LEADS_GTT src
        LEFT JOIN LEADS tgt ON src.ID = tgt.ID
        WHERE tgt.ID IS NULL
           OR tgt.NAME != src.NAME
           OR tgt.STAGE != src.STAGE
           OR tgt.CNPJ != src.CNPJ
           OR tgt.MERCADO != src.MERCADO
           OR tgt.ORIGEM != src.ORIGEM
           OR tgt.TELEFONE != src.TELEFONE
           OR tgt.END_RUA != src.END_RUA
           OR tgt.END_CEP != src.END_CEP
           OR tgt.END_NUMERO != src.END_NUMERO
           OR tgt.END_COMPLEMENTO != src.END_COMPLEMENTO
           OR tgt.END_BAIRRO != src.END_BAIRRO
           OR tgt.END_CIDADE != src.END_CIDADE
           OR tgt.END_ESTADO != src.END_ESTADO
           OR tgt.REPRESENTANTE != src.REPRESENTANTE
           OR tgt.INSCRICAO_ESTADUAL != src.INSCRICAO_ESTADUAL
           OR tgt.ANALISE_FINANCEIRA != src.ANALISE_FINANCEIRA
    """)
    differences = cursor.fetchall()    
    # Armazena as diferenças para serem usadas na próxima task
    return differences

def clean_files():
    # Move os arquivos processados para a pasta de processados
    source_directory = '/app/data/leads/processar'
    for file in os.listdir(source_directory):
        #remove files
        if file.endswith('.json') and file.startswith('validade_full'):
            os.remove(f'{source_directory}/{file}')

def apply_differences(**kwargs):
    with open('/app/data/leads/processar/validade_full_diferencas.json', 'r') as f:
        differences = json.load(f)
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    conn = oracle_hook.get_conn()
    cursor = conn.cursor()
    contador = 0
    # Executa as alterações conforme as condições
    for difference in differences:
        (
            src_id, src_name, src_stage, src_cnpj, src_mercado, src_origem, src_telefone,
            src_end_rua, src_end_cep, src_end_numero, src_end_complemento, src_end_bairro,
            src_end_cidade, src_end_estado, src_representante, src_inscricao_estadual,
            src_analise_financeira,
            tgt_id
        ) = difference

        if tgt_id is None:  # Novo registro
            INSERT = """INSERT INTO LEADS (ID, NAME, STAGE, CNPJ, MERCADO, ORIGEM, TELEFONE, END_RUA,
                    END_CEP, END_NUMERO, END_COMPLEMENTO, END_BAIRRO, END_CIDADE, END_ESTADO,
                    REPRESENTANTE, INSCRICAO_ESTADUAL,
                    ANALISE_FINANCEIRA)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17)
            """
            values = (
                src_id, src_name, src_stage, src_cnpj, src_mercado, src_origem, src_telefone,
                src_end_rua, src_end_cep, src_end_numero, src_end_complemento, src_end_bairro,
                src_end_cidade, src_end_estado, src_representante, src_inscricao_estadual,
                src_analise_financeira
            )
            print(INSERT)
            print(values)
            contador += 1
            cursor.execute(INSERT,values)

        else:  # Atualizar registro existente
            UPDATE = """
                UPDATE LEADS SET NAME = :1, STAGE = :2, CNPJ = :3, MERCADO = :4, ORIGEM = :5,
                    TELEFONE = :6, END_RUA = :7, END_CEP = :8, END_NUMERO = :9, END_COMPLEMENTO = :10,
                    END_BAIRRO = :11, END_CIDADE = :12, END_ESTADO = :13, REPRESENTANTE = :14,
                    INSCRICAO_ESTADUAL = :15,
                    ANALISE_FINANCEIRA = :16 WHERE ID = :17
            """
            values = (
                src_name, src_stage, src_cnpj, src_mercado, src_origem, src_telefone, src_end_rua,
                src_end_cep, src_end_numero, src_end_complemento, src_end_bairro, src_end_cidade,
                src_end_estado, src_representante, src_inscricao_estadual, src_analise_financeira, src_id
            )
            print(UPDATE)
            print(values)
            contador += 1

            cursor.execute(UPDATE, values)
    conn.commit()
    print('Contador: ' + str(contador))

with DAG(
    'job_validate_full',
    default_args=default_args,
    description='Sync Leads from API to Oracle',
    schedule_interval='@daily',
) as dag:

    fetch_leads = PythonOperator(
        task_id='fetch_leads',
        python_callable=fetch_leads_from_api,
    )

    insert_to_gtt = PythonOperator(
        task_id='insert_to_gtt',
        python_callable=insert_leads_to_gtt,
        op_kwargs={'leads': '{{ ti.xcom_pull(task_ids="fetch_leads") }}'},
    )

    task_apply_differences = PythonOperator(
        task_id='apply_differences',
        python_callable=apply_differences,
        op_kwargs={'differences': '{{ ti.xcom_pull(task_ids="identify_differences") }}'},
    )

    task_clean_files = PythonOperator(
        task_id='clean_files',
        python_callable=clean_files,
    )

    fetch_leads >> insert_to_gtt >> task_apply_differences >> task_clean_files
