import logging
import json
import shutil
from pathlib import Path

from git import Repo
from google.cloud import storage


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
            logging.info("Uploading: %s -> %s", str(path_to_file), destination_uri)


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


def remove_dir_if_exists(directory: Path):
    """Removes directory if exists.

    Args:
        directory (Path): Path to directory.
    """
    if directory.exists() and directory.is_dir():
        shutil.rmtree(directory)


def clone_repo_and_save_to_gcs(
    repo_url: str,
    dataform_vars: dict,
    gcs_bucket: str,
    gcs_prefix: str
):
    destination_dir = Path(Path.cwd() / "dataform")
    remove_dir_if_exists(destination_dir)

    destination_dir.mkdir(parents=True, exist_ok=True)
    Repo.clone_from(repo_url, str(destination_dir))

    dataform_json_path = destination_dir / "dataform.json"
    overwrite_dataform_vars(dataform_json_path, dataform_vars)

    gcs_destination = f"gs://{gcs_bucket}/{gcs_prefix}"
    upload_local_dir_to_gcs(destination_dir.name, gcs_destination)
    remove_dir_if_exists(destination_dir)
