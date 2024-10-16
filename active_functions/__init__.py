class ActiveFunctions:
    def __init__(self):
        import confidential
        self.url_base = confidential.BASE_URL

        self.headers = {
            "accept": "application/json",
            "Api-Token": confidential.TOKEN
        }

    def get_lead_data(self, lead_id=None, requested_data=""):
        import requests
        import json

        url = f"{self.url_base}contacts/1556524/{requested_data}"
        response = requests.get(url, headers=self.headers)
        data = json.loads(response.text)
        formatted_data = json.dumps(data, indent=4)
        return formatted_data

    def fetch_contacts(self, offset, limit):
        import requests
        params = {
            "limit": limit,
            "offset": offset
        }

        response = requests.get(f'{self.url_base}contacts', headers=self.headers, params=params)

        if response.status_code == 200:
            data = response.json()
            return data.get('contacts', [])
        else:
            print(f"Failed to fetch data at offset {offset}. Status Code: {response.status_code}")
            return []
