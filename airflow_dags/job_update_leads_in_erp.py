from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
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

dag_id = 'job_update_leads_in_erp'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25, 9, 30, 00, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

def log_inicio():
    logger.log({'msg': 'Iniciando atualizacao de leads no erp', 'dag_id': dag_id})
    


def prepare_conf(**kwargs):
    ti = kwargs['ti']
    contatos_integrar = ti.xcom_pull(key='contatos_integrar', task_ids='update_leads')
    conf = {
        "originating_dag_id": kwargs['dag'].dag_id,
        "integrar_contatos": contatos_integrar
    }
    ti.xcom_push(key='conf_data', value=conf)



def get_contatos_integrar(**kwargs):
    ti = kwargs['ti']
    contatos_integrar = ti.xcom_pull(key='contatos_integrar', task_ids='update_leads')
    conf = {"originating_dag_id": kwargs['dag'].dag_id, "integrar_contatos": contatos_integrar}
    kwargs['ti'].xcom_push(key='conf_data', value=conf)

def get_updated_leads_json_names(**kwargs):
    logger.log({'msg': 'Buscando atualizacoes de leads pendentes', 'dag_id': dag_id})

    source_directory = '/data/integration/leads/processar'
    json_files = [f for f in os.listdir(source_directory) if f.startswith('update_leads_in_erp') and f.endswith('.json')]
    if not json_files:
        raise AirflowSkipException("Não foram encontrados updates de leads pendentes de sincronização.")
    
    kwargs['ti'].xcom_push(key='updated_lead_files', value=json_files)


def monta_lead(lead, name, lead_data, key):
    if key in lead_data:
        if key == 'ZipCode':
            lead[name] = lead_data[key].replace('.', '').replace('-', '')
        elif key == 'PublicPlaceName':
            lead[name] = lead_data[key]
        elif key == 'Company':
            lead[name] = lead_data[key][:40]
        elif key == 'Phone':
            lead[name] = 0 if isinstance(lead_data[key], int) else lead_data[key]
        else:
            lead[name] = lead_data[key]

    else:
        print(f'Chave {key} não existe no dicionário lead_data')
    return lead


def get_custom_field_value(custom_fields, field_id):
    for field in custom_fields:
        if field['id'] == field_id:
            return field.get('value', '')
    print(f'Campo {field_id} não encontrado em CustomFields')
    return None


def monta_custom_fields(lead, lead_data, field_name, field_id):
    value = get_custom_field_value(lead_data.get('CustomFields', []), field_id)
    if value:
        if field_name == 'inscricao_estadual':
            lead[field_name] = str(value) if str(value) != '99' else '99'
        else:
            lead[field_name] = str(value)
    return lead


def update_leads_in_oracle(**kwargs):
    ti = kwargs['ti']
    json_files = ti.xcom_pull(key='updated_lead_files', task_ids='list_updated_leads_files')
    
    directory = '/data/integration/leads/processar'
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    
    contatos_integrar = []
    for json_file in json_files:
        file_path = os.path.join(directory, json_file)
        with open(file_path, 'r') as f:
            leads_data = json.load(f)
        
        lead = {}
        lead_data = leads_data['Lead']

        lead_stage = lead_data.get('Stage')
        if lead_stage not in ('Desenvolvimento/Orçamento', 'Ativação', 'Retenção/Expansão'):
            print(f'Lead atualizada ainda não está integrada no CRM. Etapa do lead: {lead_stage}')
            continue
        
        # TODO: VER SE EXISTE NA TABELA DE LEADS [PELO ID]

        lead = monta_lead(lead, 'stage', lead_data, 'Stage')
        lead = monta_lead(lead, 'cnpj', lead_data, 'CpfCnpj')
        lead = monta_lead(lead, 'mercado', lead_data.get('Industry', {}), 'value')
        lead = monta_lead(lead, 'origem', lead_data.get('Origin', {}), 'value')
        lead = monta_lead(lead, 'telefone', lead_data, 'Phone')
        
        '''
            A razão social e campos de endereço não devem ser atualizados pelo CRM, apenas ERP.
        '''
        lead = monta_lead(lead, 'name', lead_data, 'Company')
        lead = monta_lead(lead, 'end_cep', lead_data.get('Address', {}), 'ZipCode')
        lead = monta_lead(lead, 'end_complemento', lead_data.get('Address', {}), 'AddAddressInformation')
        lead = monta_lead(lead, 'end_cidade', lead_data.get('Address', {}), 'City')
        lead = monta_lead(lead, 'end_estado', lead_data.get('Address', {}), 'State')
        try:
            numero, rua, bairro = get_end_num(lead_data['Id'])
        except ValueError:
            print('Erro ao buscar o endereço: ')
            print(get_end_num(lead_data['Id']))

        try:
            numero = int(numero)
        except ValueError:
            numero = 0
        lead['end_rua'] = rua
        lead['end_numero'] = numero
        lead['end_bairro'] = bairro
        lead = monta_custom_fields(lead, lead_data, 'representante', '_cod.gestores')
        lead = monta_custom_fields(lead, lead_data, 'inscricao_estadual', '_inscricaoestadual')
        lead = monta_custom_fields(lead, lead_data, 'cod_cliente', '_cod.cliente')
        lead = monta_custom_fields(lead, lead_data, 'analise_financeira', '_analisefinanceira')

        # Construir a query dinamicamente
        '''
        set_clause constroi a quantidade exata de bind variables conforme as chaves do json da lead
        len(lead) + 1 o lead_id será sempre a ultima bind, no where.
        '''
        set_clause = ", ".join([f"{key} = :{i+1}" for i, key in enumerate(lead.keys())])
        update_query = f"UPDATE leads SET {set_clause} WHERE id = :{len(lead) + 1}"

        # Construir a lista de valores de bind
        row = tuple(lead[key] for key in lead.keys()) + (lead_data['Id'],)

        print('Update lead statement and values: ')
        print(update_query)
        print(row)

        # Execute the query with the row data
        try:
            logger.log({'msg': 'Fazendo update de leads no erp', 'dag_id': dag_id})

            oracle_hook.run(update_query, parameters=row)

            contatos_integrar.append(lead_data['Id'])
            if len(contatos_integrar) == 10:
                print('10 Leads atualizadas, interrompendo execução')
                break

        except Exception as e:
            logger.log({'msg': 'Erro ao atualizar leads no erp', 'dag_id': dag_id})

            print(f"Erro ao atualizar lead Lead_name: {lead_data['name']} - Lead_id: {lead_data['Id']}: {e}")
            raise e
        
        print(contatos_integrar)
        ti.xcom_push(key='integrados', value=contatos_integrar)


def get_end_num(lead_id):
    url = f'http://api.example.com:5000/endruanumero/{lead_id}'
    response = requests.get(url)
    return response.json()


def move_json_files(**kwargs):
    ti = kwargs['ti']
    logger.log({'msg': 'Movendo arquivos processados', 'dag_id': dag_id})

    source_directory = '/data/integration/leads/processar'
    destination_directory = '/data/integration/leads/processados'
    contacts_directory = '/data/integration/contacts/processar'

    integrados = ti.xcom_pull(task_ids='update_leads', key='integrados')

    json_files = ti.xcom_pull(key='updated_lead_files', task_ids='list_updated_leads_files')

    for json_file in json_files:
        source = os.path.join(source_directory, json_file)

        with open(source, 'r') as f:
            lead_data = json.load(f)
            lead_id = lead_data['Lead']['Id']
            lead_cnpj = lead_data['Lead']['CpfCnpj']
            
            if integrados:
            # Se o lead_id estiver na lista de integrados, cria um arquivo de contato na fila de processamento
                if lead_id in integrados:
                    for contato in lead_data['Lead']['Contact']:
                        contact_dict = contato
                        contact_dict['lead_cnpj'] = lead_cnpj
                        with open(os.path.join(contacts_directory, f'contato_update_erp_{lead_id}_{contact_dict["Id"]}.json'), 'w') as f:
                            json.dump(contact_dict, f)
                            print('Criado JSON de atualização de contato')
                else:
                    print(f'O lead_id id {lead_id} não está presente na lista de integrados : {integrados}')
        try:
            destination = os.path.join(destination_directory, json_file)
            shutil.move(source, destination)
            print(f"Arquivo {json_file} movido para {destination}")
        except Exception as e:
            print(f"Erro ao mover arquivo {json_file}: {e}")
            raise e
        


with DAG(dag_id,
         schedule_interval='*/3 * * * *',
         default_args=default_args,
         max_active_runs=1,
         catchup=False) as dag:
    
    list_updated_leads_files = PythonOperator(
        task_id='list_updated_leads_files',
        python_callable=get_updated_leads_json_names,
    )

    update_leads = PythonOperator(
        task_id='update_leads',
        python_callable=update_leads_in_oracle,
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

    log_start >> list_updated_leads_files >> update_leads >> move_files >> trigger_update_contatos
