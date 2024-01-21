import pandas as pd
from pandas import json_normalize
from database import Database
import requests


class DataSources:
    def __init__(self):
        db_name = 'primary.db'
        self.db_instance = Database(db_name)

    def get_weather_data(self, url):
        try:
            print(url)
            response = requests.get(url)
            weather_data = response.json()
            return weather_data
        except:
            return None

    def read_json_user_data(self, link):
        response = requests.get(link)
        users_data = response.json()
        df = pd.DataFrame(users_data)
        company_details = json_normalize(df['company'])
        company_details.columns = ['company_' + str(c) for c in list(company_details.columns)]
        df_expand = pd.concat(
            [df.drop(['address', 'company'], axis=1), json_normalize(df['address']), company_details],
            axis=1).reset_index(drop=True)
        return df_expand

    def read_sales_data(self, path_to_file):
        return pd.read_csv(path_to_file, sep=',')

    def read_weather_data(self, table_name, lat, lng, url):
        self.db_instance.connect()
        data_stored_in_db = self.db_instance.select_table(table_name)
        data_stored_in_db = data_stored_in_db[(data_stored_in_db['lat'] == lat) & (data_stored_in_db['lng'] == lng)]

        if len(data_stored_in_db) == 0:
            print('Lat and Lng are in not database')
            weather_data_from_api = self.get_weather_data(url)
            weather_data_to_insert = pd.DataFrame([{'lat': lat, 'lng': lng, 'feature': str(weather_data_from_api)}])
            self.db_instance.insert_dataframe(table_name, weather_data_to_insert, if_exists_m='append')
            self.db_instance.close_connection()
