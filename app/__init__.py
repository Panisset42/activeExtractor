import json
import threading

import active_functions
import db


def run():

    data_thread = threading.Thread(target=fetch_data)
    data_thread.start()
    while True:
        database = db.DB()
        lead_list = database.fetch_all_basic_imports()
        for lead in lead_list:
            content = input_lead_on_db(lead)
            database.insert_lead_data(**content)
        database.close()

def fetch_data():
    while True:
        database = db.DB()
        app = active_functions.ActiveFunctions()
        count = 0
        while True:
            print(f"pagina {count / 100 + 1}")
            data = app.fetch_contacts(count, 100)
            if not data:
                print("download de dados finalizado")
                break
            for line in data:
                email = line['email'].strip()
                phone = line['phone'].strip()
                first_name = line['firstName'].strip()
                lead_id = line['id'].strip()
                database.insert_basic_import(lead_id, email, phone, first_name)
            count += 100
        database.close()


def input_lead_on_db(lead):
    db.DB()
    app = active_functions.ActiveFunctions()

    lead_field_values = {}
    lead_id, lead_email, lead_phone, lead_name = lead
    # Add basic lead data
    lead_field_values['id'] = lead_id
    lead_field_values['email'] = lead_email
    lead_field_values['phone'] = lead_phone
    lead_field_values['firstName'] = lead_name
    # Fetch lead's additional data
    lead_data = json.loads(app.get_lead_data(lead_id, "fieldValues"))
    # Map the fieldValues to appropriate database columns
    for field in lead_data['fieldValues']:
        if field['field'] == '1':
            lead_field_values['telefone'] = field["value"].strip()
        if field['field'] == '10':
            lead_field_values['numero_de_funcionarios'] = field["value"].strip()
        if field['field'] == '13':
            lead_field_values['url'] = field["value"].strip()
        if field['field'] == '5':
            lead_field_values['utm_source'] = field["value"].strip()
        if field['field'] == '6':
            lead_field_values['utm_medium'] = field["value"].strip()
        if field['field'] == '7':
            lead_field_values['utm_campaign'] = field["value"].strip()
        if field['field'] == '8':
            lead_field_values['utm_term'] = field["value"].strip()
        if field['field'] == '9':
            lead_field_values['utm_content'] = field["value"].strip()
        if field['field'] == '30':
            lead_field_values['keyword'] = field["value"].strip()
        if field['field'] == '29':
            lead_field_values['cargo_na_empresa'] = field["value"].strip()
        if field['field'] == '11':
            lead_field_values['numero_de_funcionarios'] = field["value"].strip()
        if field['field'] == '12':
            lead_field_values['nome_da_empresa'] = field["value"].strip()
        if field['field'] == '27':
            lead_field_values['segmento_da_empresa'] = field["value"].strip()
        if field['field'] == '28':
            lead_field_values['o_que_busca_para_os_colaboradores'] = field["value"].strip()
        if field['field'] == '43':
            lead_field_values['palestra_ou_treinamento_da_empresa'] = field["value"].strip()
        if field['field'] == '44':
            lead_field_values['motivo_inscricao'] = field["value"].strip()
        if field['field'] == '49':
            lead_field_values['nivel_cliente'] = field["value"].strip()
        if field['field'] == '47':
            lead_field_values['salario'] = field["value"].strip()
        if field['field'] == '48':
            lead_field_values['faturamento_da_empresa'] = field["value"].strip()
    return lead_field_values
