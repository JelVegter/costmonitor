import os
from io import BytesIO

from azure.identity import ClientSecretCredential
from pandas import read_csv
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from pandas import DataFrame

# ------------------------------------------------------
# Setup
raw_data_directory = "data/raw/"
processed_data_directory = "data/processed/"
data_app = [
    "caerus",
    "hermes",
    "styx",
]

environments = ["dev", "tst", "acc", "prd"]

PRICE_COL = "costInPricingCurrency"
# ------------------------------------------------------


class ADLSConnection:
    def __init__(
        self,
        storage_account_url: str,
        container: str,
        conn_str: str = None,
        credentials: DefaultAzureCredential = None,
        environment: str = None,
    ) -> None:

        self._storage_account_url = storage_account_url
        self.container = container
        self.environment = environment
        if credentials:
            self._credentials = credentials
        if conn_str:
            self._conn_str = conn_str

    @property
    def storage_account_url(self):
        return self._storage_account_url.format(self.environment)

    @property
    def blob_service_client(self):
        if hasattr(self, "_conn_str"):
            return BlobServiceClient.from_connection_string(self._conn_str)
        return BlobServiceClient(self.storage_account_url, self._credentials)

    @property
    def container_client(self):
        return self.blob_service_client.get_container_client(self.container)

    def download_blob(self, blob_name: str) -> bytes:
        with self.blob_service_client as client:
            with client.get_blob_client(
                container=self.container, blob=blob_name
            ) as blob_client:
                download = blob_client.download_blob()
                downloaded_bytes = download.readall()
                return downloaded_bytes

    def fetch_blobs(self, blob_name_starts_with: Optional[str] = None):
        blob_iterator = self.container_client.list_blobs(
            name_starts_with=blob_name_starts_with
        )
        return blob_iterator

    def fetch_blobs_info(
        self, blob_name_starts_with: str = "", exclude_startswith: str = None
    ) -> DataFrame:

        blob_name_list = []
        creation_time_list = []
        filename_list = []

        if exclude_startswith:
            blob_list = [
                b
                for b in self.fetch_blobs(blob_name_starts_with=blob_name_starts_with)
                if not b["name"].startswith(exclude_startswith)
            ]
        else:
            blob_list = [
                b for b in self.fetch_blobs(blob_name_starts_with=blob_name_starts_with)
            ]

        for c in blob_list:
            blob_name_list.append(c["name"])
            creation_time_list.append(c["creation_time"])
            filename_list.append(get_name(c["name"]))

        return DataFrame(
            zip(blob_name_list, creation_time_list, filename_list),
            columns=["blob_name", "creation_time_adls", "filename"],
        )


def get_name(blob_name: str) -> str:
    try:
        return blob_name.split("/", 1)[1]
    except ValueError as e:
        print(f"Failed to get name from: {blob_name}")
        raise e


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


def download_export_to_csv(
    raw_data_directory: str, processed_data_directory: str, blob_names: list[str]
) -> None:
    for blob in blob_names:
        blob_file_name = blob.replace("/", "_")

        if os.path.exists(raw_data_directory + blob_file_name):
            print(f"Skipping {blob_file_name}; already exists")
            continue

        data = adls_conn.download_blob(blob)
        df = read_csv(BytesIO(data))
        df.to_csv(processed_data_directory + blob_file_name)
        print(f"Exported {blob_file_name} into {processed_data_directory}")


# Fetch raw data
df = adls_conn.fetch_blobs_info()
blobs = df["blob_name"].to_list()
download_export_to_csv(raw_data_directory, processed_data_directory, blobs)
