from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

load_dotenv()

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

#print(list(blob_service_client.list_containers()))
container_client = blob_service_client.get_container_client('tutorial')


print(container_client)
print(blob_service_client.account_url)