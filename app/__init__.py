import active_functions
import db
import threading
from time import sleep



def run():

    data_thread = threading.Thread(target=fetch_data)
    data_thread.start()

    import json
    content = json.loads(input_lead_on_db())
    with open( "data.json", "w", encoding="utf-8") as f:
        json.dump(content, f)



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
def input_lead_on_db():
    database_access = db.DB()
    app = active_functions.ActiveFunctions()
    lead_list = database_access.fetch_all_basic_imports()
    for lead in lead_list:
        lead_id, lead_email, lead_phone, lead_name = lead
        lead_data = app.get_lead_data(lead_id,"fieldValues")
        return lead_data
