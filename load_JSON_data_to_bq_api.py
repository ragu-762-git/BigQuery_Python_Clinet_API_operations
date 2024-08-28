from google.cloud import bigquery

client = bigquery.Client()

#insert the JSON data into the new table in already existing dataset
table_id = "bold-landing-432711-u5.icc_test_player_ranking_dataset.json_data"

schema = [
    bigquery.SchemaField("id", "INTEGER",mode="NULLABLE"),
    bigquery.SchemaField("first_name","STRING",mode="NULLABLE"),
    bigquery.SchemaField("last_name","STRING",mode="NULLABLE"),
    bigquery.SchemaField("dob", "STRING",mode="NULLABLE"),
    bigquery.SchemaField("addresses","RECORD",mode="REPEATED", fields=[
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("zip", "STRING"),
        bigquery.SchemaField("numberOfYears", "STRING")
    ])
]

# table = bigquery.Table(schema=schema, table_ref=table_id)
# table = client.create_table(table)

#create the job to load batch data
#create a job to load the batch data 
job_config = bigquery.LoadJobConfig(
    #skip_leading_rows = 0,
    source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
)

file_uri = "gs://bq-practice-bkt/table-1_data_new_line_delimited_json.json"

load_job = client.load_table_from_uri(destination=table_id, source_uris=file_uri, job_config=job_config)

load_job.result()


destination_table = client.get_table(table=table_id)
print("Loded table with rows {}".format(destination_table.num_rows))
