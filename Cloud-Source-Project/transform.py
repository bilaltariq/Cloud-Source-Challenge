import json
import pandas as pd
from pandas import json_normalize
from datasources import DataSources
import os

SLSH = os.path.sep
cwd = os.getcwd()

"""
This class performs transformation (if required) for raw data.
Data is read from Database, hence instance is initialize in constructor. 
"""


class TransformData:
    def __init__(self, dbinstance):
        self.db_instance = dbinstance
        self.get_data = DataSources()

    """
    Sales data transformation and insert in Database.
    """

    def transform_sales_data(self, path_to_sales_file):
        sales_data = self.get_data.read_sales_data(path_to_sales_file)
        sales_data.columns = ['sales_' + str(c) for c in sales_data.columns]
        self.db_instance.connect()
        self.db_instance.insert_dataframe('dwd_sales_data', sales_data)
        self.db_instance.close_connection()
        return sales_data

    """
    This method extracts raw data for users from API and then inserts it into Database.
    """

    def raw_users_data(self, link_to_json):
        users_data = self.get_data.read_json_user_data(link_to_json)
        users_data.columns = ['users_' + str(c) for c in users_data.columns]
        self.db_instance.connect()
        self.db_instance.insert_dataframe('dim_users', users_data)
        self.db_instance.close_connection()
        return users_data

    """
    Transformation for Users Data.
    """

    def transform_users_data(self, users_data):
        users_data['users_geo.lng'] = pd.to_numeric(users_data['users_geo.lng'], errors='coerce')
        users_data['users_geo.lat'] = pd.to_numeric(users_data['users_geo.lat'], errors='coerce')
        users_data = users_data.rename(columns={'users_geo.lng': 'users_lng'})
        users_data = users_data.rename(columns={'users_geo.lat': 'users_lat'})
        return users_data

    """
    Get weathers data using link, key and user data. 
    Lng and Log are taken from users data to fetch weather. 
    """

    def raw_weathers_data(self, link_to_api, api_key, users_data):
        lat_lng_unique_set = users_data[['users_lat', 'users_lng']]
        lat_lng_unique_set.columns = ['lat', 'lng']
        lat_lng_dict = lat_lng_unique_set.set_index('lat')['lng'].to_dict()
        for latValue, lngValue in lat_lng_dict.items():
            url = link_to_api + f"lat={latValue}&lon={lngValue}&appid={api_key}"
            self.get_data.read_weather_data(table_name="weather_info_raw", lat=latValue, lng=lngValue, url=url)

    """
    This method creates a dictionary for creating DDL. 
    Dict has keys as column name and values are data type (sqlite3 compatible)
    """

    def df_to_sqlite_dict(self, df):
        sqlite_data_types = {
            'int64': 'INTEGER',
            'float64': 'NUMERIC',
            'object': 'TEXT',
            'datetime64[ns]': 'TEXT'
        }

        columns_dict = {}
        for column, dtype in df.dtypes.items():
            sqlite_type = sqlite_data_types.get(str(dtype), 'TEXT')
            columns_dict[column] = sqlite_type

        return columns_dict

    """
    Transformation for weather data. Raw data is read from DB and transformed and saved again in DB (in new table) 
    """

    def transform_weather_data(self):

        self.db_instance.connect()
        weather_data = self.db_instance.select_table('weather_info_raw')
        self.db_instance.close_connection()

        weather_details_df = pd.DataFrame(weather_data['feature'])
        weather_details_df['feature'] = weather_details_df['feature'].apply(lambda x: json.loads(x.replace("'", '"')))
        weather_details_df = json_normalize(weather_details_df['feature']).reset_index(drop=True)
        weather_explode = json_normalize(weather_details_df['weather'].explode())
        weather_explode.columns = ['weather_' + x for x in list(weather_explode.columns)]
        weather_details_df = pd.concat([weather_explode, weather_details_df], axis=1).reset_index(drop=True)
        weather_details_df.columns = [x.replace('.', '_') for x in list(weather_details_df.columns)]
        weather_details_df = weather_details_df.astype(str)
        dict_to_create_table = self.df_to_sqlite_dict(weather_details_df)
        self.db_instance.connect()
        self.db_instance.create_table('dim_weather_details', dict_to_create_table)
        self.db_instance.insert_dataframe('dim_weather_details', weather_details_df)
        self.db_instance.close_connection()

        return weather_details_df

    """
    This method save final table, which join of Sales, Users and Weather data in Database.
    """

    def insert_final_table(self, sales_users_weather_df):
        dict_to_create_table = self.df_to_sqlite_dict(sales_users_weather_df)
        self.db_instance.connect()
        self.db_instance.create_table('final_table', dict_to_create_table)
        self.db_instance.insert_dataframe('final_table', sales_users_weather_df)
        self.db_instance.close_connection()
