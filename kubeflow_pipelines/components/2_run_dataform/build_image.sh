#!/bin/bash

# Argument parsing
while [[ "$#" -gt 0 ]]; do case $1 in
  -p|--project) project="$2"; shift;;
  -a|--author) author="$2"; shift;;
  *) echo "Unknown parameter passed: $1"; exit 1;;
esac; shift; done

[ -n "${project-}" ] || (echo "Missing required argument '--project'" && exit 1)
[ -n "${author-}" ] || (echo "Missing required argument '--author'" && exit 1)

IMAGE_NAME=run-dataform-example-${author}
IMAGE_TAG=latest

BASE_GCR_PATH=eu.gcr.io/${project}/kfp/dataform-basic-example/${author}/components
FULL_IMAGE_NAME=${BASE_GCR_PATH}/${IMAGE_NAME}:${IMAGE_TAG}

GCS_SOURCE_STAGING_DIR="${project}-staging/kfp/${IMAGE_NAME}"

# Build the Docker image using Cloud Build and store it in Cloud Registry
gcloud builds submit . \
    --tag "${FULL_IMAGE_NAME}" \
    --gcs-source-staging-dir=gs://${GCS_SOURCE_STAGING_DIR}/${author}/docker_images
