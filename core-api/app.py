from flask import Flask, request, redirect, url_for, render_template, make_response
from flask_restful import Resource, Api
import json
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
from core.models.utils import *
from time import sleep


app = Flask(__name__)
api = Api(app)

load_dotenv(dotenv_path='/path/to/app/.env')


# Diretório onde os arquivos JSON estão armazenados
JSON_DIR = '/data/app/sync/leads/processar'

# DePARA dicionário para dropdown
DEPARA_OPTIONS = {
    'Representante': '_cod.gestores',
    'Cód. Cliente': '_cod.cliente',
    'Inscrição Estadual': '_inscricaoestadual'
}

API_BASE_URL = os.getenv('API_URL')
API_TOKEN = os.getenv('API_TOKEN')

def load_jsons_by_lead_id(lead_id):
    # Varrer o diretório para encontrar todos os JSONs com o LeadId correspondente
    json_files = []
    for filename in os.listdir(JSON_DIR):
        if filename.endswith('.json') and 'converted' in filename:
            with open(os.path.join(JSON_DIR, filename), 'r') as file:
                data = json.load(file)
                # Verificar se 'Lead' é um dicionário e se possui o campo 'Id'
                if isinstance(data, dict) and 'Lead' in data and isinstance(data['Lead'], dict) and data['Lead'].get('Id') == lead_id:
                    json_files.append((data, filename))
    return json_files


class EditJson(Resource):
    def get(self):
        # Renderiza a página com o formulário e o dropdown
        response = make_response(render_template('edit_json.html', depara_options=DEPARA_OPTIONS))
        response.headers['Content-Type'] = 'text/html'
        return response
    
    def post(self):
        lead_id = int(request.form['lead_id'])
        depara_key = request.form['depara']
        custom_value = request.form['value']

        # Carregar todos os JSONs pelo LeadId
        json_files = load_jsons_by_lead_id(lead_id)
        if not json_files:
            return {"message": "Lead not found"}, 404

        # Iterar sobre todos os arquivos correspondentes e adicionar o novo campo em CustomFields
        custom_field = {"id": depara_key, "value": custom_value}
        for json_data, filename in json_files:
            json_data['Lead']['CustomFields'].append(custom_field)
            with open(os.path.join(JSON_DIR, filename), 'w') as file:
                json.dump(json_data, file, indent=4)

        # Redireciona para a página de edição com confirmação
        return redirect(url_for('editjson'))

class LeadAtualizadoApp(Resource):
    def post(self):
        try:
            data = request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            company = data.get("Lead", {}).get("Company", "")
            company = company.replace(' ', '_').replace('/', '').replace('.', '').replace(',', '')
        
            data_str = json.dumps(data, ensure_ascii=False)  
            hoje = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')

            with open(f"/data/app/sync/leads/processar/update_leads_in_erp_{company}_{hoje}.json", "w") as file:
                file.write(data_str)
            
            return 'Payload saved successfully', 200

    
class LeadDescartada(Resource):
    def post(self):
        try:
            data =request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            data_str = json.dumps(data, ensure_ascii=False)
            company = data.get("Lead", {}).get("Company", "")
            company = company.replace(' ', '_').replace('/', '').replace('.', '').replace(',', '')
            with open(f"/data/app/sync/leads/processar/lead_descartada_{company}.json", "w") as file:
                file.write(data_str)


class AtualizaLeadApp(Resource):
    
    def put(self, lead_id):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        url = API_BASE_URL + f'LeadsUpdate/{lead_id}'
        try:
            data = request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            data = json.dumps(data, ensure_ascii=True)

            response = requests.put(url, headers=headers, data=data)
            if response.status_code != 201:
                error_message = response.json()['error']['message']
                print(error_message)
                if 'Duplicated Keys' in error_message and '_cod.cliente' in error_message:
                    return '_cod.cliente duplicado', 200
                else:
                    print(data)
                    return f'Erro ao atualizar lead: {response.text} {data}', 400
            else:
                return response.json(), 201

class LeadAddInfo(Resource):
    def get(self, id):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        endpoint = API_BASE_URL + f'Leads?$filter=Id eq {id}'
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            try:
                lead = response.json()['value'][0]
                lead_end_numero = lead['number']
                lead_end_complemento = lead['complement']
                lead_end_bairro = lead['district']
                lead_end_cidade = lead['city']
                lead_end_estado = lead['state']
                lead_phone = lead['phone1']

            except Exception as e:
                return f'Error fetching lead: {e}', 400
            else:
                return {'numero': lead_end_numero, 'complemento': lead_end_complemento, 'bairro': lead_end_bairro, 'cidade': lead_end_cidade, 'estado': lead_end_estado, 'telefone': lead_phone}, 200

        else:
            return f'Error fetching lead: {response.text}', 400

        

class LeadEtapa(Resource):
    def post(self):
        try:
            data = request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            company = data.get("Lead", {}).get("Company", "")
            company = company.replace(' ', '_').replace('/', '').replace('.', '').replace(',', '')

            data_str = json.dumps(data, ensure_ascii=False)  
            hoje = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')

            with open(f"/data/app/sync/leads/processar/converted_lead_{company}_{hoje}.json", "w") as file:
                file.write(data_str)
            
            return 'Payload saved successfully', 200


class InsertedTimeline(Resource):
    def post(self):
        try:
            data = request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            #company = data.get("Lead", {}).get("Company", "")
            #company = company.replace(' ', '_').replace('/', '').replace('.', '').replace(',', '')

            data_str = json.dumps(data, ensure_ascii=False)  
            hoje = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')

            with open(f"/data/app/sync/contacts/processar/inserted_timeline_{hoje}.json", "w") as file:
                file.write(data_str)
            
            return 'Payload saved successfully', 200


class DummyContact(Resource):
    def post(self, lead_id):
        endpoint_url = f'{API_BASE_URL}personsAdd'
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        body = {
            'name': "Dummy contact",
            'leadId': lead_id
        }

        body = json.dumps(body)

        response = requests.post(endpoint_url, headers=headers, data=body)

        if response.status_code != 201:
            print(response.text)
        else:
            return 200
        

class BuscaContato(Resource):
    def get(self, lead_id):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        endpoint = API_BASE_URL + f'Persons/{lead_id}'
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            try:
                contacts = response.json()
                return contacts, 200
            except Exception as e:
                return f'Error fetching contact: {e}', 400
        else:
            return response.text, 400


class VendeLead(Resource):
    def post(self):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        try:
            data = request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            url = API_BASE_URL + f'leadsWon'
            body = json.dumps(data, ensure_ascii=True)

            response = requests.post(url, headers=headers,data=body)
            if response.status_code != 201:
                return f'Erro ao vender lead: {response.text}', 400
            else:
                return 'Lead vendido com sucesso', 200

class PassagemDeBastao(Resource):
    def post(self, lead_id):
        endpoint_url = f'{API_BASE_URL}leadsQualification'
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }

        # Example placeholder IDs - these would be stored in config or DB in a real app
        questionIds = [100001, 100002, 100003]
        answerIds = [200001, 200002, 200003]

        for i in range(0, 3):
            body = {
                "leadId": lead_id,
                "answersList": [ 
                    {
                        "questionId": questionIds[i],
                        "questionType": "2",
                        "answersIds": [
                            answerIds[i]
                        ]
                    }
                ]
            }

            response = requests.post(endpoint_url, headers=headers, data=json.dumps(body))
            if response.status_code != 201:
                print(body)
                return {'erro': response.text, 'etapa': i}, 400
            sleep(10)
            if i == 2:
                if response.status_code == 201:
                    return {'message': 'Passagem de bastão realizada com sucesso'}, 201
        
class Leads(Resource):
    def post(self):
        endpoint_url = f'{API_BASE_URL}LeadsAdd'
        try:
            data = request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            data = json.dumps(data)
            headers = {
                "Content-Type": "application/json",
                "token_exact": API_TOKEN
            }
            response = requests.post(endpoint_url, headers=headers, data=data)
            if response.status_code == 201:
                lead_id = response.json()['value']
                return {'lead_id': lead_id}, 200
            else:
                error_msg = response.json()
                return f'Error adding organization: {error_msg}', 400

    def put(self):
        try:
            data = request.get_json()
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if data is None:
                return 'Empty payload', 400
        finally:
            headers = {
                "Content-Type": "application/json",
                "token_exact": API_TOKEN
            }

            body = {"duplicityValidation": "true", "lead": dict()}
            customFields = []

            lead_id = data.get('Id')
            fields = data.get('Fields')
            for field in fields:
                f_name = field["field_name"]
                f_value = str(field["field_value"])

                if f_name[:1] == '_':
                    customFields.append({"id": f_name, "value": f_value})
                else:
                    body["lead"][f_name] = f_value

            if len(customFields):
                body["lead"]["customFields"] = customFields

            endpoint_url = f'{API_BASE_URL}LeadsUpdate/{lead_id}'

            response = requests.put(endpoint_url, headers=headers, data=json.dumps(body))
            if response.status_code == 201:
                return 'Lead updated successfully', 200
            else:
                error_message = response.json()['error']['message']
                print(error_message)
                if 'Duplicated Keys' in error_message and '_cod.cliente' in error_message:
                    return '_cod.cliente duplicado', 201

                else:
                    print(body)
                    return f'Error updating lead: {error_message}', 400


    def get(self):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        skip = 0
        endpoint_url = f'{API_BASE_URL}Leads'
        leads_list = []
        try:
            while True:
                response = requests.get(endpoint_url, headers=headers)
                leads = response.json()
                if response.status_code == 200:
                    if leads['value'] == []:
                        print(f'Leads recuperadas: {len(leads_list)}')
                        break
                    else:
                        for lead in leads['value']:
                            leads_list.append(lead)
                        skip += 500
                        endpoint_url = f'{API_BASE_URL}Leads?$skip={skip}'
            
                else:
                    return f"Error fetching leads: {leads}", response.status_code

            return leads_list, 200
        except Exception as e:
            return f'Error fetching leads: {e}', 400

class LeadId(Resource):
    def get(self, cnpj):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        endpoint = API_BASE_URL + f"Leads?$filter=cnpj eq '{cnpj}'"
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            try:
                lead = response.json()['value'][0]
                lead_id = lead['id']
                return {'id': lead_id}, 200
            except IndexError:
                return 'CNPJ não cadastrado', 404

        else:
            return response.text, 400

class AtualizaContatoApp(Resource):
    def post(self, contact_id):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        url = API_BASE_URL + f'PersonsUpdate/{contact_id}'
        try:
            print('PEGANDO O DATA')
            data = request.get_json()
            body = json.dumps(data, ensure_ascii=True)

            response = requests.put(url, headers=headers, data=body)
            if response.status_code != 200:
                error_message = response.json()['error']['message']
                print(error_message)
                return f'Erro ao atualizar contato: {response.text}', 400
            else:
                return 'Contato atualizado com sucesso!', 200
        except Exception as e:
            return f'Invalid payload: {e}', 400



class InsertContatoApp(Resource):
    def post(self):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        endpoint = API_BASE_URL + 'personsAdd'
        try: 
            data = request.get_json()
            body = json.dumps(data, ensure_ascii=True)
            print('printando data')
            print(data)
            response = requests.post(endpoint, headers=headers, data=body)
            if response.status_code != 201:
                print(response.text)
                return 'Erro ao inserir contato', 400
            else:
                return response.json(), 200
        except Exception as e:
            return f'Invalid payload: {e}', 400




class MainContact(Resource):
    def get(self, lead_id, contact_id):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        endpoint = API_BASE_URL + f'Persons/{lead_id}'
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            try:
                contacts = response.json()

                for contact in contacts['value']:
                    if contact['id'] == contact_id:
                        if contact['mainContact'] == 'true':
                            return 'S'
                        else:
                            return 'N'
                return contacts, 200
            except Exception as e:
                print(e)
                return f'Error fetching contact: {e}', 400
        else:

            return response.text, 400


class Lead(Resource):
    def get(self, id):
        try:
            lead = get_lead_by_id(id)
            if lead:
                return lead, 200
            else:
                return f'Lead with id {id} not found', 404
        except Exception as e:
            return f'Error fetching lead with id {id}: {e}', 400


class ContatoObservacao(Resource):
    def post(self, lead_id, flag):
        if flag == 'N':
            body = {"lead": {"description": "Contato atualizado - Sincronizar"}}
        elif flag == 'S':
            body = {"description": "Contato atualizado - Sincronizado"}

        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        url = API_BASE_URL + f'LeadsUpdate/{lead_id}'
        response = requests.put(url, headers=headers, data=json.dumps(body))
        if response.status_code != 201:
            return f'Erro ao inserir observacao: {response.text}', 400
        else:
            return 'Observacao inserida com sucesso', 201


class CodGestores(Resource):
    def get(self):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        cod_gestores = []
        url = API_BASE_URL + "CustomFields?$filter=key eq '_cod.gestores'"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            options = response.json()['value'][0]['options']
            for option in options:
                cod_gestores.append(option['value'])

            return cod_gestores, 200
        else:
            return response.json(), 400

class Representante(Resource):
    def get(self, lead_id):

        representante = None
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }

        url = API_BASE_URL + f"LeadsCustomFields?$filter=leadId eq {int(lead_id)}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            c_fields = response.json()['value']
            for field in c_fields:
                if field['id'] == '_cod.gestores':
                    if field['leadId'] == lead_id:
                        for option in field['options']:
                            representante = option

            
            return representante, 200
        else:
            return f'Erro ao buscar representante: {response.text}', 400


class InscricaoEstadual(Resource):
    def get(self, lead_id):
        inscr_estadual = 'ISENTO'
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        url = API_BASE_URL + f"LeadsCustomFields?$filter=leadId eq {int(lead_id)}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            c_fields = response.json()['value']
            for field in c_fields:
                if field['id'] == '_inscricaoestadual':
                    if field['leadId'] == lead_id:
                        inscr_estadual = field['value']

            return inscr_estadual, 200
        else:
            return 'Erro ao buscar inscrição estadual', 400


class RuaNumeroEndereco(Resource):
    def get(self, lead_id):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        url = API_BASE_URL + f"Leads?$filter=id eq {int(lead_id)}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            c_fields = response.json()['value'][0]
            for field in c_fields:
                if field == 'number':
                    numero = c_fields[field]
                if field == 'street':
                    rua = c_fields[field]
                if field == 'district':
                    bairro = c_fields[field]

            return (numero, rua, bairro), 200
        else:
            return 'Erro ao buscar numero', 400

class AnaliseFinanceira(Resource):
    def get(self, lead_id):
        analise_financeira = ''
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        url = API_BASE_URL + f"LeadsCustomFields?$filter=leadId eq {int(lead_id)}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            c_fields = response.json()['value']
            for field in c_fields:
                if field['id'] == '_analisefinanceira':
                    if field['leadId'] == lead_id:
                        analise_financeira = field['value']

            return analise_financeira, 200
        else:
            return 'Erro ao buscar inscrição estadual', 400

class LeadCurrentStage(Resource):
        def get(self, cnpj):
            cnpj = f"{cnpj:0>14}"
            headers = {
                "Content-Type": "application/json",
                "token_exact": API_TOKEN
            }
            endpoint = API_BASE_URL + f"Leads?$filter=cnpj eq '{cnpj}'"
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                try:
                    print(response.json())
                    lead_stage = response.json()['value'][0]['stage']
                    return lead_stage, 200
                except Exception as e:
                    return f'Error fetching lead: {e}', 400
            else:
                print('Erro: ')
                print(response.text)
                return response.text, 400
            
class InsereOrcamento(Resource):
    def post(self, lead_id):

        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        product_id = 10001  # Changed from specific ID to generic one
        existe = False
        # Verifica se existe algum orçamento, se sim, exclui ele antes de enviar o novo
        url = API_BASE_URL + f'recommendedProducts/{lead_id}'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            products = response.json()
            if products['value']:
                for product in products['value']:
                    if product['productId'] == product_id:
                        existe = True
        
        if existe:
            url = API_BASE_URL + f'recommendedProductsRemove/{lead_id}?productId={product_id}'
            delete_response = requests.delete(url, headers=headers)
            if delete_response.status_code != 204:
                return 'Erro ao deletar o  produto', 400
            
        url = API_BASE_URL + 'recommendedProductsAdd'
        try:
            data = request.get_json()
            body = json.dumps(data, ensure_ascii=True)
            response = requests.post(url, headers=headers, data=body)
            if response.status_code != 201:
                print(response.text)
                return 'Erro ao adicionar produto', 400
            else:
                return response.json(), 200
        except Exception as e:
            return f'Invalid payload: {e}', 400




class Mercados(Resource):

    def get(self):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        print(API_BASE_URL)
        endpoint_url = f'{API_BASE_URL}industries'
        try:
            response = requests.get(endpoint_url, headers=headers)
            mercados = response.json()
            if response.status_code == 200:
                return mercados
            else:
                return response.status_code, response.text
        except Exception as e:
            print(e)


    def post(self):
        try:
            mercados = request.get_json()
            print(mercados)
        except Exception as e:
            return f'Invalid payload: {e}', 400
        else:
            if mercados is None:
                return 'Empty payload', 400

            for mercado in mercados['value']:
                insere_mercados(mercado['id'], mercado['value'])
            
            return {"message": "Mercados/Segmentos inseridos com sucesso"}, 200

class LeadVendida(Resource):
    def get(self, lead_id):
        headers = {
            "Content-Type": "application/json",
            "token_exact": API_TOKEN
        }
        # Usando ODATA para filtrar por leadId no endpoint LeadsSold
        endpoint_url = API_BASE_URL + f"LeadsSold?$filter=leadId eq {lead_id}"
        response = requests.get(endpoint_url, headers=headers)
        if response.status_code == 200:
            try:
                data = response.json()
                if data.get('value'):
                    item = data['value'][0]
                    produtos = item.get('products', [])
                    leads_vendidas = []
                    for produto in produtos:
                        lead_info = {
                            'idProduto': produto.get('id'),
                            'valorProduto': produto.get('finalValue'),
                            'dataVenda': item.get('saleDate')
                        }
                        leads_vendidas.append(lead_info)
                    return leads_vendidas, 200
                else:
                    return {'message': 'Lead não encontrada ou não vendida'}, 404
            except Exception as e:
                return {'error': f'Erro ao processar a resposta: {e}'}, 400
        else:
            return {'error': response.text}, response.status_code
        

api.add_resource(AtualizaLeadApp, '/atualizalead/<int:lead_id>')
api.add_resource(LeadAtualizadoApp, '/atualizalead')
api.add_resource(LeadDescartada, '/leaddescartada')
api.add_resource(Leads, '/leads')
api.add_resource(Mercados, '/mercados')
api.add_resource(Lead, '/lead/<int:id>')
api.add_resource(LeadEtapa, '/convertedlead')
api.add_resource(LeadAddInfo, '/leadaddinfo/<int:id>')
api.add_resource(LeadId, '/leadid/<string:cnpj>')
api.add_resource(PassagemDeBastao, '/passagemdebastao/<int:lead_id>')
api.add_resource(DummyContact, '/dummycontact/<int:lead_id>')
api.add_resource(MainContact, '/maincontact/<int:lead_id>/<int:contact_id>')
api.add_resource(AtualizaContatoApp, '/atualizacontato/<int:contact_id>')
api.add_resource(ContatoObservacao, '/contatoobs/<int:lead_id>/<string:flag>')
api.add_resource(CodGestores, '/codgestores')
api.add_resource(InsertContatoApp, '/inserecontato')
api.add_resource(VendeLead, '/vendelead')
api.add_resource(InsereOrcamento, '/orcalead/<int:lead_id>')
api.add_resource(InscricaoEstadual, '/inscricaoestadual/<int:lead_id>')
api.add_resource(Representante, '/representante/<int:lead_id>')
api.add_resource(AnaliseFinanceira, '/analisefinanceira/<int:lead_id>')
api.add_resource(BuscaContato, '/buscacontato/<int:lead_id>')
api.add_resource(LeadCurrentStage, '/leadstage/<string:cnpj>')
api.add_resource(RuaNumeroEndereco, '/endruanumero/<int:lead_id>')
api.add_resource(EditJson, '/edit_json')
api.add_resource(LeadVendida, '/leadvendida/<int:lead_id>')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
