import json

import pandas as pd
from pandas import json_normalize
#from database import Database
from datasources import DataSources
import os

SLSH = os.path.sep
cwd = os.getcwd()


class TransformData:
    def __init__(self, dbinstance):
        self.db_instance = dbinstance
        self.get_data = DataSources()

    def transform_sales_data(self, path_to_sales_file):
        sales_data = self.get_data.read_sales_data(path_to_sales_file)
        self.db_instance.connect()
        self.db_instance.insert_dataframe('dwd_sales_data', sales_data)
        self.db_instance.close_connection()
        return sales_data

    def raw_users_data(self, link_to_json):
        users_data = self.get_data.read_json_user_data(link_to_json)
        #users_data = self.transform_users_data(users_data)
        self.db_instance.connect()
        self.db_instance.insert_dataframe('dim_users', users_data)
        self.db_instance.close_connection()
        return users_data

    def transform_users_data(self, users_data):
        users_data['geo.lng'] = pd.to_numeric(users_data['geo.lng'], errors='coerce')
        users_data['geo.lat'] = pd.to_numeric(users_data['geo.lat'], errors='coerce')
        users_data = users_data.rename(columns={'geo.lng': 'lng'})
        users_data = users_data.rename(columns={'geo.lat': 'lat'})
        return users_data

    def raw_weathers_data(self, link_to_api, api_key, users_data):
        lat_lng_unique_set = users_data[['lat', 'lng']]
        lat_lng_dict = lat_lng_unique_set.set_index('lat')['lng'].to_dict()
        for latValue, lngValue in lat_lng_dict.items():
            url = link_to_api + f"lat={latValue}&lon={lngValue}&appid={api_key}"
            self.get_data.read_weather_data(table_name="weather_info_raw", lat=latValue, lng=lngValue, url=url)

    def transform_weather_data(self):

        self.db_instance.connect()
        weather_data = self.db_instance.select_table('weather_info_raw')
        self.db_instance.close_connection()

        weather_details_df = pd.DataFrame(weather_data['feature'])
        weather_details_df['feature'] = weather_details_df['feature'].apply(lambda x: json.loads(x.replace("'", '"')))
        weather_details_df = json_normalize(weather_details_df['feature']).reset_index(drop=True)
        weather_explode = json_normalize(weather_details_df['weather'].explode())
        weather_explode.columns = ['weather_'+x for x in list(weather_explode.columns)]
        weather_details_df = pd.concat([weather_explode, weather_details_df],axis=1).reset_index(drop=True)
        weather_details_df.columns = [x.replace('.', '_') for x in list(weather_details_df.columns)]
        weather_details_df = weather_details_df.astype(str)
        dict_to_create_table = dict()
        for column in weather_details_df.columns:
            dict_to_create_table[column] = "TEXT"

        self.db_instance.connect()
        self.db_instance.create_table('dim_weather_details', dict_to_create_table)
        self.db_instance.insert_dataframe('dim_weather_details', weather_details_df)
        self.db_instance.close_connection()

        return weather_details_df
