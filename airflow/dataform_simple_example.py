import json
from pathlib import Path
import shutil

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

# Repository URL containing the dataform project
REPO_URL: str = "https://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:x-oauth-basic@github.com/alexanderblnf/dataform-workshop-project"

# Secret Manager Credentials name
CREDENTIALS_SECRET_NAME = "dataform_credentials"

# AUTHOR: To be modified
AUTHOR = 'alexb'

# Automatically get the project ID
_, PROJECT_ID = google.auth.default()


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
        destination_dir = Path(Path.cwd() / "dataform_example")
        LocalDiskHelper.remove_dir_if_exists(destination_dir)

        destination_dir.mkdir(parents=True, exist_ok=True)
        Repo.clone_from(repo_url, str(destination_dir))

        return destination_dir

    @staticmethod
    def create_credentials_file(base_path: Path):
        """Using the credentials in the secret manager,
        creates the required credentials file for Dataform.

        Args:
            base_path (Path): Base path where to save the credentials
        """
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


@dag(
    'dataform_simple_example',
    default_args=default_args,
    schedule_interval=None,
    tags=['dataform_example']
)
def run_basic_example():

    @task()
    def edit_dataform_file():
        config = get_current_context()['dag_run'].conf
        example_value = config.get("example_value", "default-value")
        base_dataform_folder = LocalDiskHelper.clone_dataform_project(REPO_URL)
        LocalDiskHelper.create_credentials_file(base_dataform_folder)
        file_path = base_dataform_folder / "dataform.json"

        dataform_vars = {
            "exampleValue": example_value,
            "author": AUTHOR
        }

        LocalDiskHelper.overwrite_dataform_vars(file_path, dataform_vars)

        return str(base_dataform_folder)

    dataform_folder = edit_dataform_file()
    run_dataform = BashOperator(
        task_id="run_dataform",
        bash_command=(
            f'npm i -g @dataform/cli && cd {dataform_folder} && '
            'npm install && cat dataform.json && dataform run'
        )
    )

    dataform_folder >> run_dataform


dataform_example_dag = run_basic_example()
