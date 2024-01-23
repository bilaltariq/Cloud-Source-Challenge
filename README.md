# Cloud-Source-Challenge
The project is part of Cloud Source Assignment for position of Data Engineer.

# How to set up and run the data pipeline?
1. Clone the Git repository
2. Run pip install -r requirement.txt to download all libraries.
3. Run execute main.py. [From CMD in Windows run python main.py]

+ The program fetches data from all sources, stores it in SQLite3, and provides a list of queries for user selection (see "Query Result" section). 
+ User input is only required to see query results. 
+ Type 'exit' to end the program. 
+ In order to see data schema, you can download SQLite3 UI (Link to download: https://sqlitebrowser.org/dl/), open the 'Database/primary.db' file and you can query all tables.

# Code Structure
The project is divided into four class. 
1. main.py: This class serves as the starting point for our program.
2. database.py: Utilizing SQLite3 as our database, this class dynamically handles table creation, data insertion, and data retrieval from the database
3. datasources.py: This class gets data from all data sources i.e. Sales Data, Users Data and Weather Data.
4. transform.py: This class uses the data from datasources.py and transforms/make the data is more relational form.
5. querymgmt.py: This class consists of all queries that are request, as part of the project. It uses final table that is created after transformation of all three data sources.

# Schema Details
1. 1.1 relation between Users and Weather.
2. 1.N (many) relation between Users and Sales.

Column info this diagram below.

   ![Untitled](https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/16e2d5ae-3a64-4f5e-9e3f-4d51a8dfd60f)

# Data Transformations
All transformations are available in transform.py.

1. Sales Data: No data underwent transformation; however, new columns, including qty_x_price, Year, Month, Day, and Year-Month, have been added or computed.
Method: transform.transform_sales_data()

2. User Data: The data is initially in JSON format, then it is converted into a DataFrame. During this process, all nested dictionaries are expanded to structure the data in a more relational form.
Method: transform.raw_users_data(), transform.transform_users_data()

3. Weather Data: There are two parts for weather data.
   a. API Data Retrieval: To reduce API calls, we extract unique lat-lon combinations from user data. We check if these combinations have been fetched before; if yes, no new API request is made. If not, we retrieve the data from the API and store it in the database.
   b. After finalizing the raw weather table, we follow a similar process as with the user's data, converting JSON data to a DataFrame.
Method: transform.raw_weathers_data(), transform.transform_weather_data()

# Query Results
After executing main.py, query results are displayed in the command prompt. Users can select options using integers, view the results, and download them as CSV files, saved in the 'Outputs' folder.

+ SQL scripts for each query are also saved in SQL folder.

1. Options

   
     <img width="350" alt="image" src="https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/45d19fc7-5ce2-4aea-92bf-3ad2e744aa4e">

2. Result (after select 1 in options)
   

     <img width="350" alt="image" src="https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/a5080a27-36e2-4c2f-bc98-a415fc4815d1">

3. Save as CSV


     <img width="350" alt="image" src="https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/c2ae32a6-befc-4c3b-a4bc-2f55f3dc642b">

# Visualizations [Bonus]

1. Total sales amount per customer
   
   ![newplot (4)](https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/21460892-0f11-41b5-9ab4-8037f6f7bec7)

2. Month-on-Month Sales comparison
   
   ![newplot (5)](https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/6a6053f9-a0c0-4dc5-bc75-f03c98fa92e5)

3. Month-on-Month Difference in Customers.
   
   ![newplot (6)](https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/36fcbd88-998c-4c94-bb96-dd12e666ec00)

4. Weather vs Sales Trend Month Wise
   
   ![newplot (7)](https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/73ac1d5f-0629-40c9-a35e-eeaa62fc5307)

5. Top 20 Product and their sales in each Weather.

   ![newplot (9)](https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/4c0b7a3c-32a5-4392-92b7-26832229ec93)



