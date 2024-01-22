import pandas as pd
from pandas import json_normalize
from database import Database
import requests
import os

cwd = os.getcwd()
SLSH = os.path.sep


class DataSources:
    def __init__(self):
        db_name = cwd + SLSH + 'Database' + SLSH + 'primary.db'
        self.db_instance = Database(db_name)

    """
    Input is URL for weather API.
    Return: JSON object from Weather API 
    """
    def get_weather_data(self, url):
        try:
            print(url)
            response = requests.get(url)
            weather_data = response.json()
            return weather_data
        except:
            return None

    """
    Input is User data link
    Return: Normalized Dataframe for users.
    Reads data from Json link provided. 
    """
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

    """
    Return Sales file as Dataframe 
    """
    def read_sales_data(self, path_to_file):
        return pd.read_csv(path_to_file, sep=',')

    """
    Inputs are lat, lng and url. Method first checks if lat and lng are available in Database, if not 
    and data is fetched from API. 
    Return: No returns, data is saved in DB. 
    """
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
