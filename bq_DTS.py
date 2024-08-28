from google.cloud import bigquery_datatransfer

#run this in the cloud shell before executing this script to provide authentication
#this service account has a role of bq/viewer role in source project & bq/admin role in destination project ; 
# set GOOGLE_APPLICATION_CREDENTIALS = "/home/atturragu1999/bold-landing-432711-u5-e3d6c517e304.json"


transfer_client = bigquery_datatransfer.DataTransferServiceClient()

destination_project_id = "bold-landing-432711-u5"
destination_dataset_id =  "icc_test_player_ranking_dataset"
source_project_id = "calm-vertex-432508-b1"
source_dataset_id = "employee_details"

transfer_config = bigquery_datatransfer.TransferConfig(
    destination_dataset_id = destination_dataset_id,
    display_name = "copy_dataset",
    data_source_id = "cross_region_copy",

    params = {
        source_project_id : source_project_id,
        source_dataset_id : source_dataset_id,
    },

    schedule = "every 24 hours"
)


transfer_config = transfer_client.create_transfer_config(
    parent= transfer_client.common_project_path(destination_project_id),
    transfer_config = transfer_config
)

print("Created transfer config {}".format(transfer_config.name))