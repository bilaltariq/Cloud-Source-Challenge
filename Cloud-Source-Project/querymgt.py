import pandas as pd

"""
The class consists of all query as individual methods. 
The class get initialized by df which is then used in all queries.
Transformation method creates few additional columns for analysis. 
Output for all query method is Dataframe.
"""

class QueryMgmt:
    def __init__(self, df):
        self.query_df = df

    def transformation(self):
        self.query_df['qty_x_price'] = self.query_df['sales_quantity'] * self.query_df['sales_price']
        self.query_df[['year', 'month', 'day']] = self.query_df['sales_order_date'].str.split('-', expand=True)
        self.query_df['year_month'] = self.query_df['year'] + '-' + self.query_df['month']
        self.query_df['year'] = self.query_df['year'].astype(int)
        self.query_df['month'] = self.query_df['month'].astype(int)
        self.query_df['day'] = self.query_df['day'].astype(int)
        self.query_df['quarter'] = pd.to_datetime(self.query_df[['year', 'month', 'day']]).dt.quarter
        self.query_df['year_quarter'] = (self.query_df['year'].astype(str)).str[-2:] + '-Q' + self.query_df[
            'quarter'].astype(str)

    def total_sales_per_customer(self):
        result = self.query_df.groupby('sales_customer_id')["qty_x_price"].sum().reset_index()
        return result

    def avg_order_qty_per_product(self):
        result = self.query_df.groupby('sales_product_id')['sales_quantity'].mean().reset_index()
        return result

    def top_selling_products(self):
        result = self.query_df.groupby('sales_product_id')['qty_x_price'].sum().reset_index().sort_values(by='qty_x_price',
                                                                                                    ascending=False).reset_index(
            drop=True)
        return result

    def top_customers(self):
        result = self.query_df.groupby('users_username')['qty_x_price'].sum().reset_index().sort_values(by='qty_x_price',
                                                                                                  ascending=False).reset_index(
            drop=True)
        return result

    def sales_trend_over_time(self, time_period):
        if time_period == 'Month':
            return self.query_df.groupby('year_month').agg(Count_of_Orders=('sales_order_id', 'count'),
                                                           Count_of_items=('sales_product_id', 'count'),
                                                           Sum_of_Sales=('qty_x_price', 'sum'),
                                                           Avg_Sales=('qty_x_price', 'mean'),
                                                           Number_of_Custumers=('sales_customer_id', 'count'),
                                                           Number_of_Unique_Custumers=(
                                                               'sales_customer_id', pd.Series.nunique)).sort_values(
                by='year_month', ascending=True).reset_index()

        elif time_period == 'Quarter':
            return self.query_df.groupby('year_quarter').agg(Count_of_Orders=('sales_order_id', 'count'),
                                                             Count_of_items=('sales_product_id', 'count'),
                                                             Sum_of_Sales=('qty_x_price', 'sum'),
                                                             Avg_Sales=('qty_x_price', 'mean'),
                                                             Number_of_Custumers=('sales_customer_id', 'count'),
                                                             Number_of_Unique_Custumers=(
                                                                 'sales_customer_id', pd.Series.nunique)).sort_values(
                by='year_quarter', ascending=False).reset_index()

        elif time_period == 'Year':
            return self.query_df.groupby('year').agg(Count_of_Orders=('sales_order_id', 'count'),
                                                     Count_of_items=('sales_product_id', 'count'),
                                                     Sum_of_Sales=('qty_x_price', 'sum'),
                                                     Avg_Sales=('qty_x_price', 'mean'),
                                                     Number_of_Custumers=('sales_customer_id', 'count'),
                                                     Number_of_Unique_Custumers=(
                                                     'sales_customer_id', pd.Series.nunique)).sort_values(by='year',
                                                                                                    ascending=False).reset_index()
        else:
            return pd.DataFrame()

        # self.save_option(result)
        # return result

    def sales_trend_over_weather(self):
        result = self.query_df.groupby(['sales_product_id', 'weather_description']).agg(Count_of_Orders=('sales_order_id', 'count'),
                                                                                  Count_of_items=(
                                                                                      'sales_product_id', 'count'),
                                                                                  Sum_of_Sales=('qty_x_price', 'sum'),
                                                                                  Avg_Sales=('qty_x_price', 'mean'),
                                                                                  Number_of_Custumers=(
                                                                                      'sales_customer_id', 'count'),
                                                                                  Number_of_Unique_Custumers=(
                                                                                      'sales_customer_id',
                                                                                      pd.Series.nunique)).sort_values(
            by='weather_description', ascending=False).reset_index()
        # self.save_option(result)
        return result
