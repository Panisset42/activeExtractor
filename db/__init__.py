import sqlite3
import asyncio


class DB:

    def __init__(self):
        self.con = sqlite3.connect("extractor.db")
        self.cur = self.con.cursor()

        # Create tables if they don't exist
        self.cur.execute('''CREATE TABLE IF NOT EXISTS active_basic_imports(
            id INTEGER PRIMARY KEY NOT NULL,
            email TEXT,
            phone TEXT,
            firstName TEXT
        );''')

        self.cur.execute('''CREATE TABLE IF NOT EXISTS active_lead_data(
            id INTEGER PRIMARY KEY NOT NULL,
            email TEXT,
            phone TEXT,
            firstName TEXT,
            telefone TEXT,
            conta TEXT,
            cargo TEXT,
            whatsapp TEXT,
            ddd TEXT,
            numero TEXT,
            url TEXT,
            dia_da_semana TEXT,
            nivel_cliente TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            utm_term TEXT,
            utm_content TEXT,
            keyword TEXT,
            cargo_na_empresa TEXT,
            numero_de_funcionarios TEXT,
            nome_da_empresa TEXT,
            segmento_da_empresa TEXT,
            o_que_busca_para_os_colaboradores TEXT,
            faturamento_da_empresa TEXT,
            url_lp TEXT,
            data_do_evento TEXT,
            url_grupo TEXT,
            dias_7 TEXT,
            dias_5 TEXT,
            dias_4 TEXT,
            dias_3 TEXT,
            dia_anterior TEXT,
            dia_seguinte TEXT,
            local TEXT,
            endereco TEXT,
            pto_ref TEXT,
            dia TEXT,
            cidade TEXT,
            idade TEXT,
            cidade_estado TEXT,
            trabalha TEXT,
            cargo_atual TEXT,
            tarefas_cargo TEXT,
            salario TEXT,
            resultado_significativo TEXT,
            conhecia_a_empresa TEXT,
            palestra_ou_treinamento_da_empresa TEXT,
            motivo_inscricao TEXT,
            como_ficou_sabendo TEXT,
            quanto_por_mes TEXT,
            tags TEXT
        );''')

    def insert_basic_import(self, lead_id, email, phone, first_name):
        """Insert data into the active_basic_imports table."""
        try:
            self.cur.execute('''INSERT or IGNORE INTO active_basic_imports (id,email, phone, firstName) 
                                VALUES (?, ?, ?, ?)''', (lead_id, email, phone, first_name))
            self.con.commit()


        except sqlite3.Error as e:
            print(f"An error occurred: {e}")

    def insert_lead_data(self, **kwargs):
        """Insert data into the active_lead_data table."""
        try:
            keys = ', '.join(kwargs.keys())
            placeholders = ', '.join('?' * len(kwargs))
            query = f"INSERT INTO active_lead_data ({keys}) VALUES ({placeholders})"
            self.cur.execute(query, tuple(kwargs.values()))
            self.con.commit()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")

    def fetch_all_basic_imports(self):
        """Fetch all records from the active_basic_imports table."""
        self.cur.execute('SELECT * FROM active_basic_imports')
        return self.cur.fetchall()

    def fetch_basic_import_by_id(self, record_id):
        """Fetch a record from the active_basic_imports table by ID."""
        self.cur.execute('SELECT * FROM active_basic_imports WHERE lead_id = ?', (record_id,))
        return self.cur.fetchone()

    def update_basic_import(self, record_id, email=None, phone=None, first_name=None):
        """Update a record in the active_basic_imports table."""
        fields = []
        values = []
        if email:
            fields.append("email = ?")
            values.append(email)
        if phone:
            fields.append("phone = ?")
            values.append(phone)
        if first_name:
            fields.append("firstName = ?")
            values.append(first_name)
        values.append(record_id)

        query = f"UPDATE active_basic_imports SET {', '.join(fields)} WHERE lead_id = ?"
        self.cur.execute(query, values)
        self.con.commit()

    def delete_basic_import(self, record_id):
        """Delete a record from the active_basic_imports table by ID."""
        self.cur.execute('DELETE FROM active_basic_imports WHERE lead_id = ?', (record_id,))
        self.con.commit()

    def close(self):
        """Close the database connection."""
        if self.con:
            self.con.close()
