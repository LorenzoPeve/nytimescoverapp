import azure
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import io
import os
from pdf2image import convert_from_bytes
from pymongo import MongoClient
from pymongo.collection import Collection
import requests
import time
from typing import BinaryIO
import uuid



load_dotenv()

COON_STR = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
MONGODB_CONN_STRING =  os.environ.get('MONGODB_CONN_STRING')
POPPLER_PATH = r'C:\Users\lpeve\AppData\Local\Programs\Poppler\Library\bin'

def _does_container_exist(b: BlobServiceClient, name: str) -> bool:
    """
    Checks if a container exists in the specified BlobServiceClient instance.
    """    
    for container in b.list_containers():
        if container['name'] == name:
            return True        
    return False

def _create_container(b: BlobServiceClient, name: str) -> ContainerClient:
    """Creates a new container."""

    while True:
        try:
            container = b.create_container(name)
            break
        except azure.core.exceptions.ResourceExistsError as e:
            time.sleep(5)
    return container

def initialize_container(b: BlobServiceClient) -> ContainerClient:
    if _does_container_exist(b, 'newspaper'):
        container = b.get_container_client('newspaper')
        container.delete_container()
    
    return _create_container(b, 'newspaper')

def get_ny_times_page(d: datetime) -> BinaryIO:
    """Retrieves the New York Times front page scan PDF for a specific date."""

    year, month, day = d.strftime('%Y/%m/%d').split('/')
    date = f'{year}/{month}/{day}'
    url = fr'https://static01.nyt.com/images/{date}/nytfrontpage/scan.pdf'
    response = requests.get(url)
    return response



def load(
        b: BlobServiceClient,
        c: ContainerClient,
        collection: Collection
    ) -> None:

    start_date = datetime(2015, 1, 1) # First page available at URL
    end_date = datetime.today()
    current_date = start_date
    cname = c.container_name
    not_downloaded = []

    while current_date <= end_date:

        print(current_date)

        response = get_ny_times_page(current_date)
        if response.status_code != 200:
            not_downloaded.append(current_date)
            current_date += timedelta(days=1)
            continue
        else:
            front_page = response.content

        b_name = uuid.uuid4().hex

        # Create Blob Client and upload PDF
        blob_client = b.get_blob_client(container=cname, blob= b_name+'.pdf')        
        blob_client.upload_blob(front_page)

        # # Upload PNG
        image = convert_from_bytes(
            pdf_file=front_page, poppler_path=POPPLER_PATH, dpi=200)[0]
        image_byte_io = io.BytesIO()
        image.save(image_byte_io, format='PNG')
        image_bytes = image_byte_io.getvalue()
        blob_client = b.get_blob_client(container=cname, blob=b_name+'.png')
        blob_client.upload_blob(image_bytes)

        # Add document to Mongo
        collection.insert_one({"id": b_name, "date": current_date})

        # Update date
        current_date += timedelta(days=1)



if __name__ == '__main__':
    
    client  = MongoClient(MONGODB_CONN_STRING)
    db = client['newspaper']
    db.init.delete_many({})
    collection = db.init

    blob_service_client = BlobServiceClient.from_connection_string(COON_STR)
    container = initialize_container(blob_service_client)
    load(blob_service_client, container, collection)

