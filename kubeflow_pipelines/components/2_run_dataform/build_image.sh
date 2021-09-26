#!/bin/bash

PROJECT_ID=$1
IMAGE_NAME=run-dataform-example
IMAGE_TAG=latest

BASE_GCR_PATH=eu.gcr.io/${PROJECT_ID}/kfp/dataform-basic-example/components
FULL_IMAGE_NAME=${BASE_GCR_PATH}/${IMAGE_NAME}:${IMAGE_TAG}

GCS_SOURCE_STAGING_DIR="${PROJECT_ID}-staging/kfp/${IMAGE_NAME}"

# Build the Docker image using Cloud Build and store it in Cloud Registry
gcloud builds submit . \
    --tag "${FULL_IMAGE_NAME}" \
    --gcs-source-staging-dir=gs://${GCS_SOURCE_STAGING_DIR}/docker_images
