import argparse
import os
from pathlib import Path
import shutil

from src.download_and_run_dataform import download_folder_from_gcs_and_return_base_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input-gcs-bucket",
        help="Github URL containing the Dataform Code",
        type=str,
        default="mms-dataform-builds"
    )

    parser.add_argument(
        "--input-gcs-prefix",
        help="Start date",
        type=str,
        default="dataform_folder"
    )

    args = parser.parse_args()

    base_path: Path = download_folder_from_gcs_and_return_base_path(
        gcs_bucket=args.input_gcs_bucket,
        gcs_prefix=args.input_gcs_prefix,
        local_destination_path=Path("output")
    )

    os.system(f"cd {str(base_path)} && npm install && dataform run")

    if base_path.exists() and base_path.is_dir():
        shutil.rmtree(base_path)
