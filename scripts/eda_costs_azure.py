import os
from io import BytesIO

from azure.identity import ClientSecretCredential
from pandas import read_csv

from common.datalake_utils import ADLSConnection

credentials = ClientSecretCredential(
    tenant_id="b5c47f42-c22c-453e-9984-c09cc131b040",
    client_id="f4b8c6c6-b66d-4c23-aa62-0a95ff7d6665",
    client_secret=os.getenv("CLIENT_SECRET_COSTS"),
)


adls_conn = ADLSConnection(
    environment="prd",
    storage_account_url="https://stbillingcostsprdwe.blob.core.windows.net/",
    container="azure-costs",
    credentials=credentials,
)


def download_export_to_csv(blob_names: list[str]) -> None:
    for blob in blob_names:
        blob_file_name = blob.replace("/", "_")

        if os.path.exists(blob_file_name):
            continue

        data = adls_conn.download_blob(blob)
        df = read_csv(BytesIO(data))
        df.to_csv(blob_file_name)


df = adls_conn.fetch_blobs_info()
blobs = df["blob_name"].to_list()
print("Downloading and exporting", blobs)
download_export_to_csv(blobs)
