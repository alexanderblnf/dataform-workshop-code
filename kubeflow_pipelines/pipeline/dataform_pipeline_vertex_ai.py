"""Kubeflow Pipeline for local area classifier training."""

import argparse
import logging
from pathlib import Path
from datetime import datetime

import google.auth
import kfp
from kfp.v2 import compiler
from kfp.v2.google.client import AIPlatformClient
# from google.cloud import aiplatform
# from google_cloud_pipeline_components import aiplatform as gcc_aip


_, PROJECT_ID = google.auth.default()
GCR_IMAGE_FOLDER = 'dataform-basic-example'
KFP_ROOT_GCS_PATH = f"gs://{PROJECT_ID}-staging/kfp"
GCP_REGION = "europe-west4"

load_repo_and_edit_config_op = kfp.components.load_component_from_text(f'''
inputs:
- {{name: repo_url, type: String}}
- {{name: start_date, type: String}}
- {{name: output_gcs_bucket, type: String}}
- {{name: output_gcs_prefix, type: String}}

# outputs:
# - {{name: dataform_gcs_bucket, type: String}}

implementation:
    container:
        image: eu.gcr.io/{PROJECT_ID}/kfp/{GCR_IMAGE_FOLDER}/components/load-dataform-gcs:latest
        args: [
            "--repo-url",
            {{inputValue: repo_url}},
            "--start-date",
            {{inputValue: start_date}},
            "--output-gcs-bucket",
            {{inputValue: output_gcs_bucket}},
            "--output-gcs-prefix",
            {{inputValue: output_gcs_prefix}}
        ]
''')

run_dataform_op = kfp.components.load_component_from_text(f'''
inputs:
- {{name: input_gcs_bucket, type: String}}
- {{name: input_gcs_prefix, type: String}}
implementation:
    container:
        image: eu.gcr.io/{PROJECT_ID}/kfp/{GCR_IMAGE_FOLDER}/components/run-dataform-example:latest
        args: [
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
        pipeline_root=KFP_ROOT_GCS_PATH
    )

    # Compiler().compile(train_local_area_classifier_pipeline, str(pipeline_package_path))
    # logging.info("Pipeline saved to: %s", pipeline_package_path)

    # logging.info("Uploading pipeline...")
    # client = kfp.Client(PIPELINE_HOST)
    # client.upload_pipeline(str(pipeline_package_path),
    #                        pipeline_name="Local Area Classifier Training",
    #                        description="This pipeline trains a local area classifier")


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
    # elif args.mode == 'run':
    #     create_pipeline_run()
