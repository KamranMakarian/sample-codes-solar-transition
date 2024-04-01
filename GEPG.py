
import pandas as pd
from sodapy import Socrata
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os
from io import StringIO
import logging

# Create and configure logger
logging.basicConfig(filename="logfile.log", format='%(asctime)s - %(levelname)s - %(message)s', filemode='w',
                    level=logging.DEBUG)
# Creating an object
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class AzureBlobStorageManager:
    def __init__(self, connection_str: str, container_name: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        self.container_client = self.blob_service_client.get_container_client(container_name)

    def upload_blob(self, local_path: str, blob_name=None):
        """Upload a local file to blob storage in Azure"""
        if blob_name is None:
            blob_name = os.path.basename(local_path)
        blob_client = self.container_client.get_blob_client(blob_name)

        try:
            # Upload the blob
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"Blob {blob_name} uploaded successfully.")
            logger.info(f"Blob {blob_name} uploaded successfully.")
        except Exception as e:  # Do something with this exception block (e.g. add logging)
            print(f"An error occurred: {str(e)}")
            logger.error(f"An error occurred: {str(e)}")

    def list_blobs(self, name_only=True) -> list:
        """List blobs in the container    """
        try:
            blob_list = self.container_client.list_blobs()
            logger.info("Getting list of blobs in the Azure Container")
            if name_only:
                return [blob.name for blob in blob_list]
            else:
                return list(blob_list)
        except Exception as e:  # Do something with this exception block (e.g. add logging)
            print(f"An error occurred: {str(e)}")
            logger.error(f"An error occurred: {str(e)}")
            return 0

    def get_blob_last_modified(self, blob_name: str):
        """Get the last modified time of the of blob in the container"""
        # Create a blob client
        blob_client = self.container_client.get_blob_client(blob_name)
        logger.info(f"Getting the last modified date of the {blob_name} blob")
        try:
            # Get blob properties
            blob_properties = blob_client.get_blob_properties()
            # Retrieve and print last modified date
            last_modified = blob_properties['last_modified']
            logger.info(f"The last modified date of the {blob_name} blob is : {last_modified.date()}")
            return last_modified.date()

        except Exception as e:  # Do something with this exception block (e.g. add logging)
            print(f"An error occurred: {str(e)}")
            logger.error(f"An error occurred: {str(e)}")
            return 0

    def download_blob(self, blob_name: str):
        """Download a blob from the container """
        # Create a blob client
        blob_client = self.container_client.get_blob_client(blob_name)

        try:
            # Download blob
            blob_data = blob_client.download_blob().readall()
            df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
            print(f"Blob {blob_name} downloaded successfully.")
            logger.info(f"Blob {blob_name} downloaded successfully.")
            return df

        except Exception as e:  # Do something with this exception block (e.g. add logging)
            print(f"An error occurred: {str(e)}")
            logger.error(f"An error occurred: {str(e)}")
            return 0


def Concatenate_dataset(new_data, old_data):
    """Function to concatenate two datasets """
    try:
        new_data['application_submission_date'] = pd.to_datetime(new_data['application_submission_date'])
        old_data['application_submission_date'] = pd.to_datetime(old_data['application_submission_date'])
        new_data = new_data.sort_values(by='application_submission_date')
        old_data = old_data.sort_values(by='application_submission_date')
        earliest_date = new_data['application_submission_date'].iloc[0]
        print(earliest_date)

        # Concatenate df1 to the earliest date of df2 along rows
        concatenated_df = pd.concat([old_data[old_data['application_submission_date'] < earliest_date], new_data],
                                    axis=0, ignore_index=True)
        concatenated_df = concatenated_df.sort_values(by='application_submission_date')

        logger.info('Concatenated the old data with the new data')

        return concatenated_df
    except Exception as e:  # Do something with this exception block (e.g. add logging)
        print(f"An error occurred: {str(e)}")
        logger.error(f"An error occurred: {str(e)}")
        return 0


def Get_dataset_from_DODP(dataset_identifier: str, limit: int):
    """Function to download dataset from the Delaware Open Data Portal"""
    try:
        logger.info("Getting the latest dataset from the Delaware open data portal")
        client = Socrata("data.delaware.gov", None)
        data = client.get(dataset_identifier, limit=limit)
        logger.info("Successfully feteched the latest dataset from the Delaware open data portal")
        return pd.DataFrame.from_records(data)
    except Exception as e:  # Do something with this exception block (e.g. add logging)
        print(f"An error occurred: {str(e)}")
        logger.error(f"An error occurred: {str(e)}")
        return 0


def find_most_recent_file(file_names):
    """Function to extract the date from the file name"""
    def extract_date(file_name):
        # Split the file name and extract the date part
        date_part = file_name.split("_")[-1].replace(".csv", "")
        # Convert the date string to a datetime object
        return datetime.strptime(date_part, "%Y-%m-%d")

    # Find the most recent file using the extracted date
    most_recent_file = max(file_names, key=extract_date)

    return most_recent_file


def Get_most_recent_dataset(dataset_identifier: str, limit: int, connection_string: str, container_name: str):
    """Main driver function to get the most recent GEPG dataset"""
    logger.info('Start')
    # get the api last update date
    client = Socrata("data.delaware.gov", None)
    metadata = client.get_metadata(dataset_identifier)
    timestamp = metadata['rowsUpdatedAt']
    datetime_object = datetime.utcfromtimestamp(timestamp)
    api_last_modified = datetime_object.date()

    # download the azure blob
    az_blob_manager = AzureBlobStorageManager(connection_str=connection_string, container_name=container_name)
    blobs_list = az_blob_manager.list_blobs()
    filtered_files = [file for file in blobs_list if file.startswith("Green_Energy_Program_Grants_20")]
    blob_name = find_most_recent_file(filtered_files)
    blob_last_modified = az_blob_manager.get_blob_last_modified(blob_name)
    azure_data = az_blob_manager.download_blob(blob_name)

    if (blob_last_modified):
        if blob_last_modified >= api_last_modified:
            print('No new updates from the DODP, returning the most recent data from Azure container')
            logger.info('No new updates from the DODP, returning the most recent data from Azure container')
            print(blob_name)
            logger.info('End')
            return azure_data
        elif blob_last_modified < api_last_modified:
            print('Found new updates from the DODP, returning the concatenated data')
            logger.info('Found new updates from the DODP, returning the concatenated data')
            api_data_df = Get_dataset_from_DODP(dataset_identifier, limit)
            concatenated_data = Concatenate_dataset(api_data_df, azure_data)
            today_date = datetime.now().strftime("%Y-%m-%d")
            blob_name = f"Green_Energy_Program_Grants_{today_date}.csv"
            print(blob_name)
            concatenated_data.to_csv(blob_name)
            az_blob_manager.upload_blob(blob_name)
            logger.info('End')
            return concatenated_data