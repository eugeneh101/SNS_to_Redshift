import json
import os

import boto3


DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

dynamodb_resource = boto3.resource("dynamodb")
redshift_data_client = boto3.client("redshift-data")
s3_resource = boto3.resource("s3")
sfn_client = boto3.client("stepfunctions")


def rename_s3_files(s3_prefix_old: str, s3_prefix_new: str) -> int:
    s3_bucket = s3_resource.Bucket(S3_BUCKET_NAME)
    s3_file_count = 0
    for s3_file_count, object_summary in enumerate(
        s3_bucket.objects.filter(Prefix=s3_prefix_old), 1
    ):  # uses pagination behind the scenes
        s3_file_name_old = object_summary.key
        s3_file_name_new = s3_file_name_old.replace(s3_prefix_old, s3_prefix_new)
        s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_name_new).copy_from(
            CopySource=f"{S3_BUCKET_NAME}/{s3_file_name_old}"
        )
        s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_name_old).delete()
        print(
            f"Renamed s3://{S3_BUCKET_NAME}/{s3_file_name_old} to "
            f"s3://{S3_BUCKET_NAME}/{s3_file_name_new}"
        )
    if s3_file_count:
        print(f"Successfully renamed {s3_file_count} files ✨")
    return s3_file_count


def lambda_handler(event, context) -> None:
    # print("event", event)
    redshift_queries_state = event["detail"]["state"]
    if redshift_queries_state in ["SUBMITTED", "PICKED", "STARTED"]:
        print(f"Redshift state is {redshift_queries_state}, so ignore")
        return
    redshift_queries_id = event["detail"]["statementId"]

    response = dynamodb_resource.Table(name=DYNAMODB_TABLE_NAME).query(
        IndexName="is_still_processing_sql",  # hard coded
        KeyConditionExpression=boto3.dynamodb.conditions.Key("redshift_queries_id").eq(
            redshift_queries_id
        ),
        # Select='ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT',
        # AttributesToGet=['string'],
    )
    assert response["Count"] == 1, (
        f'For `redshift_queries_id` "{redshift_queries_id}", there should be exactly 1 record '
        f"but got {response['Count']} records. The records are: {response['Items']}"
    )
    record = response["Items"][0]
    task_token = record["task_token"]
    if redshift_queries_state == "FINISHED":
        print(f"Succeeded with `redshift_queries_id` {redshift_queries_id}")
        sfn_client.send_task_success(
            taskToken=task_token,
            output='"json output of the task"',  # figure out what to write here such as completed statementId, table name
        )
        num_queries = len(
            json.loads(record["sql_queries"])
        )  # assumes that 'select count(*)' is last query
        row_count = redshift_data_client.get_statement_result(
            Id=f"{redshift_queries_id}:{num_queries}"
        )["Records"][0][0]["longValue"]
        dynamodb_resource.Table(name=DYNAMODB_TABLE_NAME).update_item(
            Key={
                "full_table_name": record["full_table_name"],
                "utc_now_human_readable": record["utc_now_human_readable"],
            },
            # REMOVE is more important than SET. REMOVE deletes attribute that
            # makes record show up in the table's GSI.
            UpdateExpression="REMOVE #isp SET row_count = :rc",
            ExpressionAttributeValues={":rc": row_count},
            ExpressionAttributeNames={"#isp": "is_still_processing_sql?"},
        )
        s3_prefix_processing = record["s3_prefix_processing"]
        s3_prefix_processed = s3_prefix_processing.replace(
            "/processing/", "/processed/"  # hard coded
        )
        rename_s3_files(
            s3_prefix_old=s3_prefix_processing, s3_prefix_new=s3_prefix_processed
        )
    elif redshift_queries_state in ["ABORTED", "FAILED"]:
        print(
            f"Failed with `redshift_queries_id` {redshift_queries_id} with state being {redshift_queries_state}"
        )
        sfn_client.send_task_failure(
            taskToken=task_token,
            error='"string"',  # figure out what to write here
            cause='"string"',  # figure out what to write here
        )
    else:  # 'ALL'
        print(
            f"Failed with `redshift_queries_id` {redshift_queries_id} with state being {redshift_queries_state}"
        )
        sfn_client.send_task_failure(
            taskToken=task_token,
            error=f'"`redshift_queries_state` is {redshift_queries_state}"',  # figure out what to write here
            cause='"string"',  # figure out what to write here
        )
