import os
import json

from dotenv import load_dotenv
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv(r'D:\\recommendation-system-ppp\\.env')

SUBSCRIPTION_ID = os.getenv('AZURE_SUBSCRIPTION_ID', None)
RESOURCE_GROUP_NAME = os.getenv('RESOURCE_GROUP_NAME')
LOCATION = os.getenv('LOCATION')
STORAGE_ACCOUNT_NAME = os.getenv('STORAGE_ACCOUNT_NAME')
LIST_CONTAINER_NAMES = json.loads(os.getenv('LIST_CONTAINER_NAMES'))

RESOURCE_CLIENT = ResourceManagementClient(
    AzureCliCredential(),
    subscription_id=SUBSCRIPTION_ID
)

STORAGE_CLIENT = StorageManagementClient(
    AzureCliCredential(),
    subscription_id=SUBSCRIPTION_ID
)

def create_resource_group():
    """
    https://learn.microsoft.com/en-us/azure/developer/python/sdk/examples/azure-sdk-example-resource-group
    :return:
    """
    rg_result = RESOURCE_CLIENT.resource_groups.create_or_update(
        RESOURCE_GROUP_NAME,
        {"location": LOCATION}
    )
    print(f"Created resource group {rg_result.name}")
def create_storage_account():
    """
    https://learn.microsoft.com/es-es/azure/developer/python/sdk/examples/azure-sdk-example-storage?tabs=bash
    :return:
    """
    availability_result = STORAGE_CLIENT.storage_accounts.check_name_availability(
        {"name": STORAGE_ACCOUNT_NAME}
    )
    if not availability_result.name_available:
        print(f"Storage name {STORAGE_ACCOUNT_NAME} is already in use. Try another name.")
        exit()
    poller = STORAGE_CLIENT.storage_accounts.begin_create(
        RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME,
        {
            "location": LOCATION,
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"},
            "hierarchical-namespace": True
        }
    )
    account_result = poller.result()
    print(f"Provisioned storage account {account_result.name}")
def get_datalake_service_client():
    keys = STORAGE_CLIENT.storage_accounts.list_keys(
        RESOURCE_GROUP_NAME,
        STORAGE_ACCOUNT_NAME
    )
    print(f"Primary key for storage account: {keys.keys[0].value}")

    conn_string = f"DefaultEndpointsProtocol=https;" \
                  f"AccountName={STORAGE_ACCOUNT_NAME};" \
                  f"AccountKey={keys.keys[0].value};" \
                  f"EndpointSuffix=core.windows.net"
    client = DataLakeServiceClient.from_connection_string(conn_string)
    return client
def create_file_system(client: DataLakeServiceClient, file_name_system):
    """
    https://learn.microsoft.com/es-es/python/api/overview/azure/storage-file-datalake-readme?view=azure-python
    :param client:
    :param file_name_system:
    :return:
    """
    client.create_file_system(file_system=file_name_system)
    print(f"Created file system: {file_name_system}")

if __name__ == '__main__':
    create_resource_group()
    create_storage_account()
    datalake_service_client = get_datalake_service_client()
    for container in LIST_CONTAINER_NAMES:
        create_file_system(datalake_service_client, container)
