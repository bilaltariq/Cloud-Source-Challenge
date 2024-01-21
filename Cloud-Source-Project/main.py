import pandas as pd
import os
from database import Database
from transform import TransformData
from querymgt import QueryMgmt

SLSH = os.path.sep
cwd = os.getcwd()

db_name = 'primary.db'
db_instance = Database(db_name)
db_instance.initialize_tables()
transform_data = TransformData(dbinstance=db_instance)


def worker(sales_file_path, link_to_json, weatherApi, key):
    sales_data = transform_data.transform_sales_data(path_to_sales_file=sales_file_path)

    users_data = transform_data.raw_users_data(link_to_json=link_to_json)
    users_data = transform_data.transform_users_data(users_data)

    sales_users_data = sales_data.merge(users_data, how='left', left_on='customer_id', right_on='id')

    transform_data.raw_weathers_data(link_to_api=weatherApi, api_key=key, users_data=users_data)
    weather_details_df = transform_data.transform_weather_data()

    weather_details_df['coord_lon'] = pd.to_numeric(weather_details_df['coord_lon'], errors='coerce')
    weather_details_df['coord_lat'] = pd.to_numeric(weather_details_df['coord_lat'], errors='coerce')
    sales_user_weather_df = pd.merge(sales_users_data, weather_details_df, how='left', left_on=['lng', 'lat']
                                     , right_on=['coord_lon', 'coord_lat'])

    return sales_user_weather_df


def save_option(df):
    save_choice = input("Do you want to save the result as a CSV file? (yes/no): ").lower()
    if save_choice == 'yes':
        file_name = input("Enter the CSV file name (without extension): ")
        df.to_csv(f"{file_name}.csv", index=False)
        print(f"Result saved as {file_name}.csv")


def main():
    sales_file_path = cwd + SLSH + 'sales_data.csv'
    link_to_json = "https://jsonplaceholder.typicode.com/users"
    weatherApi = "https://api.openweathermap.org/data/2.5/weather?"
    key = "b251aa99ea5b80b71785ff34d7da8056"

    sales_user_weather_df = worker(sales_file_path, link_to_json, weatherApi, key)
    get_query_result = QueryMgmt(sales_user_weather_df)
    get_query_result.transformation()

    queries = {
        '1': get_query_result.total_sales_per_customer(),
        '2': get_query_result.avg_order_qty_per_product(),
        '3': get_query_result.top_selling_products(),
        '4': get_query_result.top_customers(),
        '5': get_query_result.sales_trend_over_time(time_period='Year'),
        '6': get_query_result.sales_trend_over_time(time_period='Quarter'),
        '7': get_query_result.sales_trend_over_time(time_period='Month'),
        '8': get_query_result.sales_trend_over_weather(),
    }

    while True:
        print("\nOptions:")
        print("1. Calculate total sales amount per customer.")
        print("2. Determine the average order quantity per product.")
        print("3. Identify the top-selling products.")
        print("4. Identify the customers.")
        print("5. Yearly sales trends")
        print("6. Quarterly sales trends")
        print("7. Monthly sales trends")
        print("8. Product sales with respect to Weather")
        print("9. Save result as CSV.")
        print("Type 'exit' to end the program")

        # Get user input
        user_input = input("Enter your choice: ")

        if user_input.lower() == 'exit':
            print("Exiting the program.")
            break  # Exit the loop if user types 'exit'

        elif user_input in queries:
            result = queries[user_input]
            if result is not None:
                print(result)
                save_option(result)
            else:
                print('No result returned.')

        else:
            print("Invalid input. Please enter a valid option or 'exit'.")


if __name__ == '__main__':
    main()
