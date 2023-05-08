import json
import os
from datetime import datetime, timedelta

import boto3


DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
DYNAMODB_TTL_IN_DAYS = json.loads(os.environ["DYNAMODB_TTL_IN_DAYS"])
FILE_TYPE = os.environ["FILE_TYPE"]
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_CLUSTER_NAME"]
REDSHIFT_COPY_ADDITIONAL_ARGUMENTS = os.environ["REDSHIFT_COPY_ADDITIONAL_ARGUMENTS"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_ROLE_NAME = os.environ["REDSHIFT_ROLE_NAME"]
REDSHIFT_SCHEMA_NAME = os.environ["REDSHIFT_SCHEMA_NAME"]
REDSHIFT_SECRET_NAME = os.environ["REDSHIFT_SECRET_NAME"]
REDSHIFT_TABLE_NAME = os.environ["REDSHIFT_TABLE_NAME"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
TRUNCATE_TABLE = json.loads(os.environ["TRUNCATE_TABLE"])

dynamodb_resource = boto3.resource("dynamodb")
iam_client = boto3.client("iam")
redshift_data_client = boto3.client("redshift-data")
secrets_manager_client = boto3.client("secretsmanager")


def lambda_handler(event, context) -> None:
    # print(f"event: {event}")
    redshift_manifest_file_name = event["redshift_manifest_file_name"]
    s3_prefix_processing = event["s3_prefix_processing"]
    task_token = event["task_token"]
    sql_queries = []
    if TRUNCATE_TABLE:
        sql_queries.append(
            f"truncate {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME};"
        )
    redshift_role_arn = iam_client.get_role(RoleName=REDSHIFT_ROLE_NAME)["Role"]["Arn"]
    sql_queries.extend(
        [
            f"""
        copy {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME}
        from 's3://{S3_BUCKET_NAME}/{s3_prefix_processing}'
        iam_role '{redshift_role_arn}'
        format as {FILE_TYPE} {REDSHIFT_COPY_ADDITIONAL_ARGUMENTS};
        """,  ### eventually put REDSHIFT_DATABASE_NAME, REDSHIFT_SCHEMA_NAME, REDSHIFT_TABLE_NAME as event payload
            f"select count(*) from {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME};",
        ]
    )
    redshift_secret_arn = secrets_manager_client.describe_secret(
        SecretId=REDSHIFT_SECRET_NAME
    )["ARN"]
    response = redshift_data_client.batch_execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        SecretArn=redshift_secret_arn,
        Database=REDSHIFT_DATABASE_NAME,
        Sqls=sql_queries,
        WithEvent=True,
    )
    # print(response)
    response.pop(
        "CreatedAt", None
    )  # has a datetime() object that is not JSON serializable
    utc_now = datetime.utcnow()
    records_expires_on = utc_now + timedelta(days=DYNAMODB_TTL_IN_DAYS)
    dynamodb_resource.Table(name=DYNAMODB_TABLE_NAME).put_item(
        Item={
            "full_table_name": f"{REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME}",  # pk in primary index
            "utc_now_human_readable": utc_now.strftime("%Y-%m-%d %H:%M:%S")
            + " UTC",  # sk in primary index
            "redshift_queries_id": response["Id"],  # pk in GSI
            "is_still_processing_sql?": "yes",  # sk in GSI
            "task_token": task_token,
            "sql_queries": json.dumps(sql_queries),
            "redshift_manifest_file_name": redshift_manifest_file_name,  # needed by `redshift_queries_finished_lambda`
            "redshift_response": json.dumps(response),
            "delete_record_on": int(records_expires_on.timestamp()),
            "delete_record_on_human_readable": records_expires_on.strftime(
                "%Y-%m-%d %H:%M:%S" + " UTC"
            ),
        }
    )
