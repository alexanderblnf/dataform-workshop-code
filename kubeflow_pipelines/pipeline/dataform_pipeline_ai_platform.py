"""Kubeflow Pipeline for local area classifier training."""

import argparse
import logging
from pathlib import Path
from datetime import datetime

import google.auth
import kfp
from kfp.compiler import Compiler


_, PROJECT_ID = google.auth.default()
GCR_IMAGE_FOLDER = 'dataform-basic-example'
KFP_ROOT_GCS_PATH = f"gs://{PROJECT_ID}-staging/kfp"
GCP_REGION = "europe-west4"
PIPELINE_HOST = "https://2bb39853f6fbed9-dot-europe-west1.pipelines.googleusercontent.com/"


def load_repo_and_edit_config_op(
    repo_url: str,
    start_date: str,
    output_gcs_bucket: str,
    output_gcs_prefix: str,
):
    return kfp.dsl.ContainerOp(
        name="save_dataform_repo_to_gcs",
        image=(
            f"eu.gcr.io/{PROJECT_ID}/kfp/{GCR_IMAGE_FOLDER}/components/load-dataform-gcs:latest"
        ),
        arguments=[
            "--repo-url",
            repo_url,
            "--start-date",
            start_date,
            "--output-gcs-bucket",
            output_gcs_bucket,
            "--output-gcs-prefix",
            output_gcs_prefix,
        ],
    )


def run_dataform_op(
    input_gcs_bucket: str,
    input_gcs_prefix: str
):
    """ContainerOp to preprocess training data.

        Args:
            training_data_source_gcs_path (str): Cloud Storage path where
                the raw training data is stored.
            output (str): Local output path that contains the return values.
        """

    return kfp.dsl.ContainerOp(
        name="run_dataform_example",
        image=(
            f"eu.gcr.io/{PROJECT_ID}/kfp/{GCR_IMAGE_FOLDER}/"
            "components/run-dataform-example:latest"
        ),
        arguments=[
            "--input-gcs-bucket",
            input_gcs_bucket,
            "--input-gcs-prefix",
            input_gcs_prefix
        ]
    )


@kfp.dsl.pipeline(
    name='Dataform Simple Example',
    description='This pipeline loads a dataform project from Github and runs it.'
)
def dataform_simple_example_pipeline(
    repo_url: str = "https://github.com/alexanderblnf/dataform-workshop-sample",
    output_gcs_bucket: str = "mms-dataform-builds",
    output_gcs_prefix: str = "dataform_folder",
):
    # 1. Load training data from BigQuery
    load_repo_and_edit_config_step = load_repo_and_edit_config_op(
        repo_url=repo_url,
        start_date="test",
        output_gcs_bucket=output_gcs_bucket,
        output_gcs_prefix=output_gcs_prefix
    ).set_display_name('Load Repository and Save to GCS Bucket')
    load_repo_and_edit_config_step.execution_options.caching_strategy.max_cache_staleness = "P0D"

    # 2. Validate training data
    run_dataform_step = run_dataform_op(
        # input_gcs_bucket=load_repo_and_edit_config_step.outputs['output_gcs_buckest'],
        # input_gcs_prefix=load_repo_and_edit_config_step.outputs['output_gcs_prefix']
        input_gcs_bucket=output_gcs_bucket,
        input_gcs_prefix=output_gcs_prefix
    ).after(load_repo_and_edit_config_step).set_display_name('Run Dataform example')
    run_dataform_step.execution_options.caching_strategy.max_cache_staleness = "P0D"


def compile_and_upload_pipeline():
    """Convenience function to compile and upload the pipeline"""
    logging.info("Compiling pipeline...")
    package_dir = Path("./pipeline-packages-ai-platform/")
    current_date_and_time = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
    pipeline_package_path = (
        package_dir / f"dataform-simple-example-pipeline-{current_date_and_time}.zip"
    )
    pipeline_package_path.parent.mkdir(parents=True, exist_ok=True)

    Compiler().compile(
        dataform_simple_example_pipeline,
        str(pipeline_package_path)
    )

    logging.info("Uploading pipeline...")
    client = kfp.Client(PIPELINE_HOST)
    try:
        client.upload_pipeline(
            str(pipeline_package_path),
            pipeline_name="Dataform Simple Example",
            description="Pipeline that runs Dataform"
        )
    except Exception:
        client.upload_pipeline_version(
            str(pipeline_package_path),
            pipeline_name="Dataform Simple Example",
            pipeline_version_name=str(current_date_and_time)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        help=("Mode to use the pipeline with. "
              "'run' submit a pipeline job and 'compile' compiles and uploads "
              "the pipeline"),
        choices=['compile', 'run'],
        default='compile',
        type=str,
    )
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    if args.mode == 'compile':
        compile_and_upload_pipeline()
