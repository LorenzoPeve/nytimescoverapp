import azure
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import requests
import time
from typing import BinaryIO

load_dotenv()

def does_container_exist(b: BlobServiceClient, name: str):
    """
    Checks if a container exists in the specified BlobServiceClient instance.
    """    
    for container in b.list_containers():
        if container['name'] == name:
            return True        
    return False






connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)


if does_container_exist(blob_service_client, 'newspaper'):  

    container = blob_service_client.get_container_client('newspaper')
    container.delete_container()

while True:
    try:
        container = blob_service_client.create_container('newspaper')
        break
    except azure.core.exceptions.ResourceExistsError as e:
        print('hfe')
        time.sleep(2)

print('here')


def get_ny_times_page(d: str) -> BinaryIO:
    """Retrieves the New York Times front page scan PDF for a specific date."""

    year, month, day = d.strftime('%Y/%m/%d').split('/')
    date = f'{year}/{month}/{day}'
    url = fr'https://static01.nyt.com/images/{date}/nytfrontpage/scan.pdf'
    response = requests.get(url)
    return response




# start_date = datetime(2023, 8, 6)
# end_date = datetime.today()

# current_date = start_date
# while current_date <= end_date:

#     blob_name = current_date.strftime('%Y_%m_%d')
#     front_page = get_ny_times_page

#     blob_client = blob_service_client.get_blob_client(
#         container=container.container_name, blob=blob_name)
#     blob_client.upload_blob(front_page)

#     current_date += timedelta(days=1)




# blob_list = container.list_blobs()
# for blob in blob_list:
#     print(blob.name)




