# CREATING AN EXTERNAL TABLE IN BQ AND QUERYING ; DATA IS STORED IN GCS BUCKET
from google.cloud import bigquery

client = bigquery.Client()

project_id = "bold-landing-432711-u5"
dataset_id = "icc_test_player_ranking_dataset"

#create a referecene for the BQ dataset for the exteranl table to map
dataset_ref = bigquery.DatasetReference(dataset_id=dataset_id, project=project_id)
table_id = "employee_details"

#define the schema 

schema = [
    bigquery.SchemaField("first_name" , "STRING", mode="NULLABLE"),
    bigquery.SchemaField("last_name" , "STRING", mode="NULLABLE"),
    bigquery.SchemaField("job_title" , "STRING", mode="NULLABLE"),
    bigquery.SchemaField("department" , "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email" , "STRING", mode="NULLABLE"),
    bigquery.SchemaField("address" , "STRING", mode="NULLABLE"),
    bigquery.SchemaField("phone_number" , "STRING", mode="NULLABLE"),
    bigquery.SchemaField("salary" , "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("password" , "STRING", mode="NULLABLE"),
]


#config to create a table 
table = bigquery.Table(dataset_ref.table(table_id=table_id), schema= schema)
external_config = bigquery.ExternalConfig("CSV")

#gcs path where the source file is located 
external_config.source_uris =[
    "gs://bq-practice-bkt/employee_data.csv"
]

#skip header row 
external_config.options.skip_leading_rows = 1 

table.external_data_configuration = external_config

#create a permanent table linked to the GCS file 
#make an api request
table = client.create_table(table=table)


