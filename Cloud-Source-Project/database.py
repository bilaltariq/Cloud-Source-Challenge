import sqlite3
import pandas as pd


class Database:
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = None

    def connect(self):
        try:
            # Connect to the SQLite database. If it does not exist, it will be created.
            self.connection = sqlite3.connect(self.db_name)
            # print(f"Connected to SQLite database: {self.db_name}")
        except sqlite3.Error as e:
            print(f"Error: Unable to connect to SQLite database. {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            # print(f"Connection to SQLite database {self.db_name} closed.")
            self.connection = None

    def create_table(self, table_name, columns_dict):
        try:
            # Use a cursor to execute SQL commands
            cursor = self.connection.cursor()

            # Construct the CREATE TABLE query
            columns_str = ', '.join([f"{col_name} {col_desc}" for col_name, col_desc in columns_dict.items()])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str});"

            # Execute the query
            cursor.execute(create_table_query)
            print(f"Table {table_name} created (if not exists).")

            # Commit the changes and close the cursor
            self.connection.commit()
            cursor.close()

        except sqlite3.Error as e:
            print(f"Error: Unable to create table {table_name}. {e}")

    def insert_dataframe(self, table_name, dataframe, if_exists_m='append'):
        try:
            # Use the to_sql method to insert the DataFrame into the database
            dataframe.to_sql(table_name, self.connection, if_exists=if_exists_m, index=False)
            print(f"Data inserted into table {table_name}.")

            # Commit the changes
            self.connection.commit()

        except sqlite3.Error as e:
            print(f"Error: Unable to insert data into table {table_name}. {e}")

    def select_table(self, table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            result_df = pd.DataFrame(rows, columns=columns)
            cursor.close()
            return result_df

        except sqlite3.Error as e:
            print(f"Error: Unable to execute SELECT query on table {table_name}. {e}")
            return None

    def initialize_tables(self):
        self.connect()
        sales_table = "dwd_sales_data"
        columns_dict_sales_table = {"id": "INTEGER PRIMARY KEY", "order_id": "INTEGER", "customer_id": "INTEGER",
                                    "product_id": "INTEGER", "quantity": "INTEGER", "price": "DECIMAL",
                                    "order_date": "DATE"}
        self.create_table(sales_table, columns_dict_sales_table)

        users_data_table = "dim_users"
        columns_dict_users_data_table = {"id": "INTEGER", "name": "TEXT", "username": "TEXT", "email": "TEXT",
                                         "phone": "TEXT", "website": "TEXT", "street": "TEXT", "suite": "TEXT",
                                         "city": "TEXT", "zipcode": "TEXT", "lat": "DECIMAL", "lng": "DECIMAL",
                                         "name": "TEXT", "catchPhrase": "TEXT", "bs": "TEXT"}
        self.create_table(users_data_table, columns_dict_users_data_table)

        weather_table = "weather_info_raw"
        columns_dict_weather_table = {"id": "INTEGER PRIMARY KEY", "lat": "DECIMAL", "lng": "DECIMAL",
                                      "feature": "TEXT"}
        self.create_table(weather_table, columns_dict_weather_table)

        self.close_connection()