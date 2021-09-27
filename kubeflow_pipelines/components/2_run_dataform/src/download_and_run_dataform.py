import shutil
import json
from pathlib import Path
from google.cloud import storage

from src.secret_helper import SecretManagerHelper

CREDENTIALS_SECRET_NAME = "dataform_credentials"


def create_credentials_file(project_id: str, base_path: Path):
    secret_manager_helper = SecretManagerHelper(project_id=project_id)
    credentials = json.loads(
        secret_manager_helper.get_secret(CREDENTIALS_SECRET_NAME)
    )
    file_path = base_path / ".df-credentials.json"

    with open(str(file_path), 'w') as json_file:
        json.dump(credentials, json_file, indent=4)


def download_folder_from_gcs_and_return_base_path(
    project_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    local_destination_path: Path
):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_or_name=gcs_bucket)
    blobs = bucket.list_blobs(prefix=gcs_prefix)

    base_path = Path(local_destination_path / Path(gcs_prefix))

    if base_path.exists() and base_path.is_dir():
        shutil.rmtree(base_path)

    for blob in blobs:
        file_path = Path(local_destination_path / blob.name)

        if len(file_path.parts) > 1:
            base_path = Path(*file_path.parts[:-1])
            base_path.mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(file_path)

    create_credentials_file(project_id, base_path)

    return base_path
