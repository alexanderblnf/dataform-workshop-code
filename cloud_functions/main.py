"""This file contains the main Cloud Function code."""
import time
import json
import requests
import logging
import os
from google.cloud import secretmanager
import google.auth
from google.cloud import storage

_, PROJECT_ID = google.auth.default()

DATAFORM_PROJECT_ID = "5650608764747776"

AUTHOR = os.environ["AUTHOR"]


class SecretManagerHelper:
    # pylint: disable=too-few-public-methods
    """Wrapper around Google Secret Manager."""

    project_id: str
    client: secretmanager.SecretManagerServiceClient

    def __init__(self, project_id: str):
        """Sets project and instantiates Secret Manager client"""
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()

    def get_secret(self, secret_name: str) -> str:
        """Using the secret name, fetches the secret value

        Args:
            secret_name (str): The name of the secret as defined in
                Google Secret Manager

        Returns:
            str: The value stored in the secret
        """
        name = (
            f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        )

        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")


class DataformAPIHelper:

    API_KEY_SECRET_NAME = "dataform_api_key"

    def __init__(self, gcp_project_id: str, dataform_project_id: str) -> None:
        self.api_key = SecretManagerHelper(gcp_project_id).get_secret(self.API_KEY_SECRET_NAME)
        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        self.base_url = f'https://api.dataform.co/v1/project/{dataform_project_id}/run'

    def trigger_run(self):
        response = requests.post(
            url=self.base_url,
            data="{}",
            headers=self.headers
        )

        return response.json()['id']

    def wait_for_finish(self, run_id: str):
        run_url = f"{self.base_url}/{run_id}"
        response = requests.get(run_url, headers=self.headers)

        while response.json()['status'] == 'RUNNING':
            # Check every 5 seconds
            time.sleep(5)
            response = requests.get(run_url, headers=self.headers)
            logging.warning(response.json())

    def execute_run(self):
        run_id = self.trigger_run()
        self.wait_for_finish(run_id)


def download_gcs_file(project_id: str, bucket: str, path: str):
    # Instantiate a Google Cloud Storage client and specify required bucket and file
    storage_client = storage.Client(project_id)
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(path)
    blob_content = blob.download_as_text()
    print(blob_content)
    # Download the contents of the blob as a string and then parse it using json.loads() method
    return json.loads(blob_content)


def execute_dataform_run_alexb(event: dict, _):
    """Background Cloud Function to be triggered by Cloud Storage.
    Args:
        event (dict):  The dictionary with data specific to this type of event.
    """
    bucket = event['bucket']
    path = event['name']

    # Check that file is in the AUTHOR folder and it's of JSON format
    if AUTHOR in path and path.endswith('.json'):
        json_content = download_gcs_file(
            project_id=PROJECT_ID,
            bucket=bucket,
            path=path
        )
        print(json_content)
        DataformAPIHelper(PROJECT_ID, DATAFORM_PROJECT_ID).execute_run()
