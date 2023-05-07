import os
from typing import Union

import boto3

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_BUCKET_PREFIX = os.environ["S3_BUCKET_PREFIX"]
s3 = boto3.resource("s3")


def rename_s3_files(s3_prefix_old: str, s3_prefix_new: str) -> int:
    s3_bucket = s3.Bucket(S3_BUCKET_NAME)
    s3_file_count = 0
    for s3_file_count, object_summary in enumerate(
        s3_bucket.objects.filter(Prefix=s3_prefix_old), 1
    ):  # uses pagination behind the scenes
        s3_file_name_old = object_summary.key
        s3_file_name_new = s3_file_name_old.replace(s3_prefix_old, s3_prefix_new)
        s3.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_name_new).copy_from(
            CopySource=f"{S3_BUCKET_NAME}/{s3_file_name_old}"
        )
        s3.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_name_old).delete()
        print(
            f"Renamed s3://{S3_BUCKET_NAME}/{s3_file_name_old} to "
            f"s3://{S3_BUCKET_NAME}/{s3_file_name_new}"
        )
    if s3_file_count:
        print(f"Successfully renamed {s3_file_count} files ✨")
    return s3_file_count


def lambda_handler(event, context) -> dict[str, Union[str, dict[str, str]]]:
    s3_prefix_processing = S3_BUCKET_PREFIX.replace(
        "/unprocessed/", "/processing/"  # hard coded
    )
    s3_prefix_unexpected = s3_prefix_processing.replace(
        "/processing/", "/__should_have_been_processed_but_not__/"  # hard coded
    )
    s3_file_count = rename_s3_files(
        s3_prefix_old=s3_prefix_processing,
        s3_prefix_new=s3_prefix_unexpected,
    )
    if s3_file_count:
        print(
            f"There should be 0 files in {S3_BUCKET_PREFIX} but got "
            f"{s3_file_count} files, so moved them to {s3_prefix_unexpected}"
        )
    rename_s3_files(
        s3_prefix_old=S3_BUCKET_PREFIX,
        s3_prefix_new=s3_prefix_processing,
    )
    return {
        "s3_prefix_processing": s3_prefix_processing,
        "other_metadata": {
            "s3_prefix_unprocessed": S3_BUCKET_PREFIX,
            "s3_prefix_unexpected": s3_prefix_unexpected,
        },
    }
