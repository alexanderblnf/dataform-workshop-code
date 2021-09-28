import json
from pathlib import Path
import shutil
import os

from git import Repo
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from google.cloud import secretmanager, storage
import google.auth


default_args = {
    'owner': 'airflow',
}

REPO_URL: str = "ADD_REPO_URL_HERE"

CREDENTIALS_SECRET_NAME = "dataform_credentials"

AUTHOR = 'alexb'

_, PROJECT_ID = google.auth.default()

GCS_BUCKET = f"{PROJECT_ID}-dataform-build"

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


@dag(
    'dataform_audience_example',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['dataform_example']
)
def run_audience_example():

    @task()
    def upload_repo_to_gcs():
        # TODO: Get variable from global variables
        config = get_current_context()['dag_run'].conf
        example_value = config.get("example_value", "default-value")
        base_dataform_folder = LocalDiskHelper.clone_dataform_project(REPO_URL)
        LocalDiskHelper.create_credentials_file(base_dataform_folder)

        file_path = base_dataform_folder / "dataform.json"

        dataform_vars = {
            "exampleValue": example_value,
            "author": AUTHOR,
            "isAudienceEnabled": "true",
        }

        LocalDiskHelper.overwrite_dataform_vars(file_path, dataform_vars)
        GCSHelper.upload_local_dir_to_gcs(base_dataform_folder, GCS_PATH)

        return {
            "bucket": GCS_BUCKET,
            "path": AUTHOR
        }

    @task()
    def download_and_execute(gcs_payload: dict):
        gcs_bucket = gcs_payload["bucket"]
        gcs_path = gcs_payload["path"]

        local_destination_path = Path.cwd() / "gcs_example"
        final_base_path = GCSHelper.download_folder_from_gcs_and_return_base_path(
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_path,
            local_destination_path=local_destination_path
        )

        run_dataform = BashOperator(
            task_id="run_dataform",
            bash_command=f'npm i -g @dataform/cli && pwd && cd {str(final_base_path)} && npm install && cat dataform.json && dataform run --tags orchestrator_audience'
        )
        run_dataform.execute({})

    payload = upload_repo_to_gcs()
    result = download_and_execute(payload)

    payload >> result


dataform_example_dag = run_audience_example()
