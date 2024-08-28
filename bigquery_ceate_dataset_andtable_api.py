from google.cloud import bigquery

#construct a bigquery client
client = bigquery.Client()

#set dataset id to the ID of the dataset object to create 
dataset_id = "{}.icc_test_player_ranking_dataset".format(client.project)

#construct the full dataset object to send to the api
dataset = bigquery.Dataset(dataset_id)

#specify the geographic location o the dataset to reside
dataset.location = "asia-south1"

#send the dataset to the API for creation wih an explicit timeout
#Raises google.api_core.exceptions if the dataset already exists within the project
dataset = client.create_dataset(dataset, timeout=30) #Make an API request
print("Created dataset {},{}.".format(client.project, dataset.dataset_id))

#Table Creation

table_id = "bold-landing-432711-u5.icc_test_player_ranking_dataset.test_innings_rank"

schema = [
    bigquery.SchemaField("Rank", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("PlayerName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Country", "STRING", mode="NULLABLE")
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table) #make an api request

print("Created table {},{},{}".format(table.project, table.dataset_id,table.table_id))

#create a job to load the batch data 
job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 0,
    source_format = bigquery.SourceFormat.CSV
)

file_uri = "gs://bq-practice-bkt/batsmen_rankings_test.csv"

#load the job
load_job = client.load_table_from_uri(source_uris=file_uri, job_config=job_config, destination=table_id)

#make an api request
load_job.result() # waits for the job to complete

destination_table = client.get_table(table) # make an api request
print("Loaded {} rows".format(destination_table.num_rows))



