import requests
import os


class ApiClient:
    def __init__(self):
        self.base_url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
        self.header = {
            "X-Nickname": os.getenv('X-Nickname'),
            "X-Cohort": os.getenv('X-Cohort'),
            "X-API-KEY": os.getenv('X-API-KEY')
        }

    def get_data(self, endpoint, params):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, headers=self.header, params=params)
            response.raise_for_status()  
            return response.json()
        except Exception as e:
            print(f"Error fetching data from {endpoint}: {e}")
            return None







    
