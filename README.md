# Cloud-Source-Challenge
The project is part of Cloud Source Assignment. 

# How to set up and run the data pipeline?
Clone the git repository and run main.py. Just need to make sure the 'sales_data.csv' is available in current working directory. 

# Code Structure
The project is divided into four class. 
1. main.py: This class is the point where our program will start to run.
2. database.py: Since we are using sqlite3 as our datatase, this class have dynamic method to creating tables, inserting datatables and select data from database.
3. datasources.py: This class gets data from all data sources i.e. Sales Data, Users Data and Weather Data.
4. transform.py: This class uses the data from datasources.py and transforms/make the data is more relational form.
5. querymgmt.py: This class consists of all queries that are request, as part of the project. It uses final table that is created after transformation of all three data sources.

# Schema Details
1. 1.1 relation between Users and Weather.
2. 1.N (many) relation between Users and Sales.
Column info this diagram below.

![Untitled](https://github.com/bilaltariq/Cloud-Source-Challenge/assets/10683094/16e2d5ae-3a64-4f5e-9e3f-4d51a8dfd60f)

# Data Transformations

# Query Results / Visuals







