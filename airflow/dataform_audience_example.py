import json
from pathlib import Path
import shutil

from git import Repo
from airflow.decorators import dag, task
from google.cloud import secretmanager, storage
import google.auth


default_args = {
    'owner': 'airflow',
}

# Repository URL containing the dataform project
REPO_URL: str = "https://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:x-oauth-basic@github.com/alexanderblnf/dataform-workshop-project"

# Secret Manager Credentials name
CREDENTIALS_SECRET_NAME = "dataform_credentials"

# AUTHOR: To be modified
# You can set the author in a global variable in Airflow and retrieve it from there
# TODO: Add the author from a global variable
AUTHOR = 'CHANGE-THE-AUTHOR-HERE'

# Automatically get the project ID
_, PROJECT_ID = google.auth.default()


# Bucket where the dataform build should be saved
GCS_BUCKET = f"{PROJECT_ID}-dataform-build"

# Full GCS path
GCS_PATH = f"gs://{GCS_BUCKET}/{AUTHOR}"


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


class LocalDiskHelper:
    @staticmethod
    def clone_dataform_project(repo_url: str):
        """Loads dataform project from Github into a local folder

        Args:
            repo_url (str): Github https path to repository

        Returns:
            destination_dir (Path): Path to where the project will be saved.
        """
        destination_dir = Path("dataform_example")
        LocalDiskHelper.remove_dir_if_exists(destination_dir)

        destination_dir.mkdir(parents=True, exist_ok=True)
        Repo.clone_from(repo_url, str(destination_dir))

        return destination_dir

    @staticmethod
    def create_credentials_file(base_path: Path):
        secret_manager_helper = SecretManagerHelper(PROJECT_ID)
        credentials = json.loads(
            secret_manager_helper.get_secret(CREDENTIALS_SECRET_NAME)
        )
        file_path = base_path / ".df-credentials.json"

        with open(str(file_path), 'w') as json_file:
            json.dump(credentials, json_file, indent=4)

    @staticmethod
    def remove_dir_if_exists(directory: Path):
        """Removes directory if exists.

        Args:
            directory (Path): Path to directory.
        """
        if directory.exists() and directory.is_dir():
            shutil.rmtree(directory)

    @staticmethod
    def overwrite_dataform_vars(dataform_json_path: str, dataform_vars: dict):
        """Overwrites dataform variables in the given dataform_json_path

        Args:
            dataform_json_path (str): Path pointing towards dataform.json file
            dataform_vars (dict): Variables to add / change in dataform.json
        """
        with open(dataform_json_path) as json_file:
            json_data = json.load(json_file)

        if "vars" not in json_data:
            json_data["vars"] = {}

        json_data["vars"] = {
            **json_data["vars"],
            **dataform_vars
        }

        with open(dataform_json_path, 'w') as out_file:
            json.dump(json_data, out_file, indent=4)


class GCSHelper:
    """Contains functions to upload and download from GCS."""

    @staticmethod
    def upload_local_dir_to_gcs(local_dir_path, destination_gcs_path):
        """Upload the contents of a local directory to GCS

        Note: The structure of the local directory is replicated on Cloud Storage.

        Args:
            local_dir_path (str): The path to the local directory.
            destination_gcs_path (str): The path to the GCS location.
        """
        client = storage.Client()
        local_dir_path = Path(local_dir_path)
        for path_to_file in local_dir_path.glob("**/*"):
            if path_to_file.is_file():
                path_without_root = Path(*path_to_file.parts[1:])
                destination_uri = f"{destination_gcs_path}/{path_without_root}"
                destination_blob = storage.blob.Blob.from_string(destination_uri,
                                                                 client=client)
                destination_blob.upload_from_filename(path_to_file)

    @staticmethod
    def download_folder_from_gcs_and_return_base_path(
        gcs_bucket: str,
        gcs_prefix: str,
        local_destination_path: Path
    ):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_or_name=gcs_bucket)
        blobs = bucket.list_blobs(prefix=gcs_prefix)

        base_path = Path(local_destination_path / Path(gcs_prefix))

        LocalDiskHelper.remove_dir_if_exists(base_path)

        for blob in blobs:
            file_path = Path(local_destination_path / blob.name)

            if len(file_path.parts) > 1:
                base_path = Path(*file_path.parts[:-1])
                base_path.mkdir(parents=True, exist_ok=True)

            blob.download_to_filename(file_path)

        return base_path


# TODO: Add DAG configuration
def run_audience_example():

    @task()
    def upload_repo_to_gcs():
        # TODO 1: Clone the repo, add the credentials and edit the config file.
        # Example value can be added as a global variable
        # TODO 2: Add the repo to GCS. BE MINDFUL OF THE AUTHOR!

        return {
            "bucket": GCS_BUCKET,
            "path": AUTHOR
        }

    @task()
    def download_and_execute(gcs_payload: dict):
        gcs_bucket = gcs_payload["bucket"]
        gcs_path = gcs_payload["path"]

        # TODO 3: Download the repo from GCS
        # TODO 4: Execute Dataform with the correct tag
        # Tip: You can execute predefined Operators inside tasks

    payload = upload_repo_to_gcs()
    result = download_and_execute(payload)

    payload >> result

# TODO 5: Run the actual DAG
