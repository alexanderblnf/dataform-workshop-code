#!/bin/bash

# Saner programming env: these switches turn some bugs into errors
set -o errexit -o pipefail -o noclobber -o nounset

# Argument parsing
while [[ "$#" -gt 0 ]]; do case $1 in
  -a|--author) author="$2"; shift;;
  *) echo "Unknown parameter passed: $1"; exit 1;;
esac; shift; done

[ -n "${author-}" ] || (echo "Missing required argument '--author'" && exit 1)

BUCKET="da-concepts-dev-gcs-workshop"

gcloud functions deploy execute_dataform_run_${author} \
--set-env-vars AUTHOR=${author} \
--runtime python38 \
--region europe-west1 \
--trigger-resource ${BUCKET} \
--trigger-event google.storage.object.finalize
