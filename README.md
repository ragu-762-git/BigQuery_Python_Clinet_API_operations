# BigQuery_Python_Clinet_API_operations
This repository contains python files where each file contains individual operations such as creating a dataset, table and loading batch data into BigQuery using the Bigquery Client API library written in python.

# Objective 
The objective of this repo is to learn how to do all operations in Bigquery starting from creating a dataset & table and loading data into the table (overwriting the table, append data to the table) and run SQL queries in the BigQuery Client API in Python itself. 

# note 
1. Before running the scripts in the cloud shell, make sure the dataset and table are present in the BQ and if not, create the dataset, table and load the bacth data in the code itself.
2. Before running the scripts make sure you have necessary permission in your service account that you're using. For example, for transfering (copy) a dataset in BQ from one project to other, create a new service account in your destination project and assign the role of BQ admin. Then take this created principal in the destination project and assign it viewer role. Download the service account's key and set the authentication using the bash command export GOOGLE_APPLICATION_CREDENTIALS = "path to the json file key". Then run the python script. 
