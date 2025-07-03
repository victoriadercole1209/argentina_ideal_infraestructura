from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "usm-infra-grupo5"
client = bigquery.Client(project=PROJECT_ID)

def create_bigquery_dataset(dataset_id: str, location="US"):
    ref = client.dataset(dataset_id)
    try:
        client.get_dataset(ref)
        print(f"El dataset '{dataset_id}' ya existe.")
    except NotFound:
        dataset = bigquery.Dataset(ref)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' creado correctamente.")