"""Kubeflow Pipeline for local area classifier training."""

import argparse
import logging
from pathlib import Path
from datetime import datetime

import google.auth
import kfp
from kfp.v2 import compiler
from kfp.v2.google.client import AIPlatformClient
from secret_helper import SecretManagerHelper


_, PROJECT_ID = google.auth.default()
GCR_IMAGE_FOLDER = 'dataform-basic-example'

GITHUB_CREDENTIALS_SECRET_NAME = "workshop_github_access_token"
GITHUB_ACCESS_TOKEN = (
    SecretManagerHelper(PROJECT_ID)
    .get_secret(GITHUB_CREDENTIALS_SECRET_NAME)
)
REPO_URL = (
    f"https://{GITHUB_ACCESS_TOKEN}:"
    "x-oauth-basic@github.com/alexanderblnf/dataform-workshop-project"
)

KFP_ROOT_GCS_PATH = f"gs://{PROJECT_ID}-staging/kfp/vertex-ai"
GCP_REGION = "europe-west4"
GCS_BUCKET = f"{PROJECT_ID}-dataform-build"

global author

# TODO: Add author param


def compile_and_upload_pipeline():
    load_repo_and_edit_config_op = kfp.components.load_component_from_text(f'''
    inputs:
    - {{name: repo_url, type: String}}
    - {{name: example_value, type: String}}
    - {{name: output_gcs_bucket, type: String}}
    - {{name: output_gcs_prefix, type: String}}

    implementation:
        container:
            image: eu.gcr.io/{PROJECT_ID}/kfp/{GCR_IMAGE_FOLDER}/{author}/components/load-dataform-gcs-{author}:latest
            args: [
                "--repo-url",
                {{inputValue: repo_url}},
                "--example-value",
                {{inputValue: example_value}},
                "--output-gcs-bucket",
                {{inputValue: output_gcs_bucket}},
                "--output-gcs-prefix",
                {{inputValue: output_gcs_prefix}}
            ]
    ''')

    run_dataform_op = kfp.components.load_component_from_text(f'''
    inputs:
    - {{name: project_id, type: String}}
    - {{name: input_gcs_bucket, type: String}}
    - {{name: input_gcs_prefix, type: String}}
    implementation:
        container:
            image: eu.gcr.io/{PROJECT_ID}/kfp/{GCR_IMAGE_FOLDER}/{author}/components/run-dataform-example-{author}:latest
            args: [
                "--project-id",
                {{inputValue: project_id}},
                "--input-gcs-bucket",
                {{inputValue: input_gcs_bucket}},
                "--input-gcs-prefix",
                {{inputValue: input_gcs_prefix}}
            ]
    ''')

    @kfp.dsl.pipeline(
        name='dataform-simple-example',
        description='This pipeline loads a dataform project from Github and runs it.',
        pipeline_root=KFP_ROOT_GCS_PATH
    )
    def dataform_simple_example_pipeline(
        repo_url: str = REPO_URL,
        example_value: str = "vertex-ai-value",
        output_gcs_bucket: str = GCS_BUCKET,
        output_gcs_prefix: str = "dataform_folder",
        # TODO: Add author param
    ):
        # 1. Load training data from BigQuery
        # TODO: Add author param
        load_repo_and_edit_config_step = load_repo_and_edit_config_op(
            repo_url=repo_url,
            example_value=example_value,
            output_gcs_bucket=output_gcs_bucket,
            output_gcs_prefix=f"{author}/{output_gcs_prefix}"
        ).set_display_name('Load Repository and Save to GCS Bucket')
        load_repo_and_edit_config_step.execution_options.caching_strategy.max_cache_staleness = "P0D"

        # 2. Validate training data
        run_dataform_step = run_dataform_op(
            project_id=PROJECT_ID,
            input_gcs_bucket=output_gcs_bucket,
            input_gcs_prefix=f"{author}/{output_gcs_prefix}"
        ).after(load_repo_and_edit_config_step).set_display_name('Run Dataform example')
        run_dataform_step.execution_options.caching_strategy.max_cache_staleness = "P0D"

    """Convenience function to compile and upload the pipeline"""
    logging.info("Compiling pipeline...")
    package_dir = Path("./pipeline-packages/")
    current_date_and_time = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
    pipeline_package_path = (
        package_dir / f"dataform-simple-example-pipeline-{current_date_and_time}.json"
    )
    pipeline_package_path.parent.mkdir(parents=True, exist_ok=True)

    compiler.Compiler().compile(
        pipeline_func=dataform_simple_example_pipeline,
        package_path=str(pipeline_package_path))

    api_client = AIPlatformClient(project_id=PROJECT_ID, region=GCP_REGION)

    api_client.create_run_from_job_spec(
        str(pipeline_package_path),
        pipeline_root=KFP_ROOT_GCS_PATH,
        enable_caching=False
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--author",
        help=("Author of the pipeline."),
        type=str,
        required=True
    )
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    author = args.author
    compile_and_upload_pipeline()
