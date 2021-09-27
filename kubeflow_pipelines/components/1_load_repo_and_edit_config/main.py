import argparse

from src.load_and_save_to_gcs import clone_repo_and_save_to_gcs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--repo-url",
        help="Github URL containing the Dataform Code",
        type=str,
    )

    parser.add_argument(
        "--example-value",
        help="Example value",
        type=str,
        required=True
    )

    # TODO: Add author param

    parser.add_argument(
        "--output-gcs-bucket",
        help="Output GCS bucket for saving the Dataform project",
        type=str,
    )

    parser.add_argument(
        "--output-gcs-prefix",
        help="GCS path to store the loaded training data",
        type=str,
    )

    args = parser.parse_args()

    # TODO: Add author
    dataform_vars = {
        "exampleValue": args.example_value
    }

    clone_repo_and_save_to_gcs(
        repo_url=args.repo_url,
        dataform_vars=dataform_vars,
        gcs_bucket=args.output_gcs_bucket,
        gcs_prefix=args.output_gcs_prefix
    )
