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

dag_id = 'job_vende_leads_in_crm'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

def log_inicio():
    logger.log({'msg': 'Iniciando atualizacao de leads no erp', 'dag_id': dag_id})
    


def buscar_lead_vendida_in_erp():
    logger.log({'msg': 'Buscando leads vendidas no erp', 'dag_id': dag_id})
    sql = "select * from view_leads_vendidas"
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    leads = oracle_hook.get_records(sql)

    return leads


def vende_lead_in_crm():

    leads = buscar_lead_vendida_in_erp()
    if not leads:
        raise AirflowSkipException('Nenhuma lead vendida encontrada')
    
    headers = {'Content-Type': 'application/json'}
    url = 'http://api.example.com:5000/vendelead'
    for lead in leads:
        lead_id = lead[0]
        valor_venda = lead[1]
        #email_rep = lead[2]
        product_id = 10001
        body = {
            "leadId": lead_id,
            #"salesRepEmail": email_rep,
            "salesRepEmail": "sales@example.com",
            "ignoreSalesRep": False,
            "products": [
                {
                    "productId": product_id,
                    "quantity": 1,
                    "labelValue": valor_venda
                }
            ]
        }


        body = json.dumps(body)
        response = requests.post(url, headers=headers, data=body)
        
        if response.status_code != 200:
            print('Erro. Verificando causa.')
            if 'Lead is sold' in response.text:
                print('Lead ja vendida')
                print(f'Vendendo a lead no ERP...')
                oracle_hook = OracleHook(oracle_conn_id='oracle_db')
                sql_venda_erp = f'select valor_pedido from vw_crm_clientes where id_crm = {lead_id}'
                valor_venda = oracle_hook.get_records(sql_venda_erp)[0][0]
                update = f"update leads set valor_pedido = :1 where id = :2"
                row = (valor_venda, lead_id)
                oracle_hook.run(update, parameters=row)
                logger.log({'msg': f'Lead {lead_id} vendida com sucesso', 'dag_id': dag_id})

            else:
                raise KeyError(f'Erro ao vender a lead {lead_id}: ' + response.text)

        else:
            oracle_hook = OracleHook(oracle_conn_id='oracle_db')
            update = f"update leads set valor_pedido = :1 where id = :2"
            row = (valor_venda, lead_id)
            oracle_hook.run(update, parameters=row)
            logger.log({'msg': f'Lead {lead_id} orcada com sucesso', 'dag_id': dag_id})
            


with DAG(dag_id,
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         max_active_runs=1,
         tags=['vendas', 'leads'],
         catchup=False) as dag:

    log_inicio = PythonOperator(
        task_id = 'task_log_inicio',
        python_callable=log_inicio,
        provide_context=True
    )

    vende_lead_in_crm = PythonOperator(
        task_id = 'task_vende_lead_in_crm',
        python_callable=vende_lead_in_crm,
        provide_context=True
    )

    log_inicio >> vende_lead_in_crm
