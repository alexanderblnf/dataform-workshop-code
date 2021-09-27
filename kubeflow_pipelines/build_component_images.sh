# !/bin/bash

# Saner programming env: these switches turn some bugs into errors
set -o errexit -o pipefail -o noclobber -o nounset

# Argument parsing
while [[ "$#" -gt 0 ]]; do case $1 in
  -p|--project) project="$2"; shift;;
  -a|--author) author="$2"; shift;;
  *) echo "Unknown parameter passed: $1"; exit 1;;
esac; shift; done

[ -n "${project-}" ] || (echo "Missing required argument '--project'" && exit 1)
[ -n "${author-}" ] || (echo "Missing required argument '--author'" && exit 1)

cd "components/1_load_repo_and_edit_config/" && ./build_image.sh --project ${project} --author ${author}
cd -

cd "components/2_run_dataform" && ./build_image.sh --project ${project} --author ${author}
cd -
