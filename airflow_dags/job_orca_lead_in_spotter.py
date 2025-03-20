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

local_tz = pendulum.timezone("America/Sao_Paulo")

dag_id = 'job_budget_leads_in_crm'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

def log_inicio():
    logger.log({'msg': 'Iniciando busca por orcamento de leads no erp', 'dag_id': dag_id})
    

def buscar_lead_orcada_in_erp():
    logger.log({'msg': 'Buscando leads orcadas no erp', 'dag_id': dag_id})
    sql = "select * from view_leads_orcadas"
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    leads = oracle_hook.get_records(sql)

    return leads


def busca_etapa_atual(lead_cnpj):
    url = 'http://api.example.com:5000/leadstage/' + str(lead_cnpj)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print(response.json())
        return response.json()


def soma_produtos(lead_id):
    headers = {'Content-Type': 'application/json'}
    url = f'http://api.example.com:5000/leadvendida/{lead_id}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        produtos = response.json()
        total_valor_produtos = sum(produto['valorProduto'] if produto['valorProduto'] is not None else 0 for produto in produtos)
        if total_valor_produtos == None:
            total_valor_produtos = 0
        return total_valor_produtos
    else:
        print(f'Erro ao obter produtos para lead {lead_id}: {response.text}')
        raise KeyError('Erro ao obter produtos para lead {lead_id}')

def orca_lead_in_crm():

    leads = buscar_lead_orcada_in_erp()
    if not leads:
        raise AirflowSkipException('Nenhuma lead orçada encontrada')
    
    headers = {'Content-Type': 'application/json'}
    for lead in leads:

        lead_id = lead[0]
        valor_orcamento = lead[1]
        cnpj = lead[2]
        product_id = 10001

        url = f'http://api.example.com:5000/orcalead/{lead_id}'

        # Body request
        body = {
            "leadId": lead_id,
            "products": [
                {
                    "productId": product_id,
                    "quantity": 1,
                    "labelValue": valor_orcamento
                }
            ]
        }

        body = json.dumps(body)

        response = requests.post(url, headers=headers, data=body)
        
        if response.status_code != 200:
            if "Erro ao adicionar produto" in response.text:
                print('ERRO\nBuscando a etapa da lead no CRM...')
                etapa_atual = busca_etapa_atual(cnpj)
                print(f'Etapa atual: {etapa_atual}')
                if etapa_atual == 'Retenção/Expansão' or etapa_atual == 'Ativação':
                    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
                    print('Esta lead ja foi vendida... Atualizando a tabela de leads e valores de orcamento e venda.')
                    print('Buscando o valor dos produtos vendidos...')
                    total_valor_produtos = soma_produtos(lead_id)

                    print(f'Total valor produtos: {total_valor_produtos}')
                    print(f'Atualizando o valor no banco de dados...')

                    print(f'Marcando a lead como orçada.')
                    sql_orcamento_erp = f"select valor_orcamento from vw_crm_clientes where id_crm = {lead_id}"
                    valor_orcamento = oracle_hook.get_records(sql_orcamento_erp)[0][0]
                    update = f"update leads set valor_orcamento = :1 where id = :2"
                    row = (valor_orcamento, lead_id)
                    oracle_hook.run(update, parameters=row)
                    logger.log({'msg': f'Lead {lead_id} orcada com sucesso', 'dag_id': dag_id})

                    print(f'Atualizando a etapa atual...')
                    row_leads_table = (etapa_atual, lead_id)
                    update = f"update leads set stage = :1 where id = :2"
                    oracle_hook.run(update, parameters=row_leads_table)
                    print('Finalizado com sucesso ESTA LEAD')

            else:
                raise KeyError('Erro ao orcar a lead. ' + response.text)

        else:
            # Atualiza o  valor orçado na tabela de leads;
            oracle_hook = OracleHook(oracle_conn_id='oracle_db')
            update = f"update leads set valor_orcamento = :1 where id = :2"
            row = (valor_orcamento, lead_id)
            oracle_hook.run(update, parameters=row)
            logger.log({'msg': f'Lead {lead_id} orcada com sucesso', 'dag_id': dag_id})


with DAG(dag_id,
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         max_active_runs=1,
         tags=['orcamento', 'leads'],
         catchup=False) as dag:

        get_leads_orcadas = PythonOperator(
            task_id = 'task_busca_leads_orcadas',
            python_callable=buscar_lead_orcada_in_erp,
            provide_context=True
        )

        orca_leads = PythonOperator(
            task_id = 'task_orca_leads_no_crm',
            python_callable=orca_lead_in_crm,
            provide_context=True
        )

        get_leads_orcadas >> orca_leads
