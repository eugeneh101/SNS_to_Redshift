import os
import time

import boto3


REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_CLUSTER_NAME"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_SCHEMA_NAME = os.environ["REDSHIFT_SCHEMA_NAME"]
REDSHIFT_SECRET_NAME = os.environ["REDSHIFT_SECRET_NAME"]
REDSHIFT_TABLE_NAME = os.environ["REDSHIFT_TABLE_NAME"]

redshift_data_client = boto3.client("redshift-data")
secrets_manager_client = boto3.client("secretsmanager")


def execute_sql_statement(sql_statement: str) -> None:
    redshift_secret_arn = secrets_manager_client.describe_secret(
        SecretId=REDSHIFT_SECRET_NAME
    )["ARN"]
    response = redshift_data_client.execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        SecretArn=redshift_secret_arn,
        Database=REDSHIFT_DATABASE_NAME,
        Sql=sql_statement,
    )
    time.sleep(1)
    while True:
        response = redshift_data_client.describe_statement(Id=response["Id"])
        status = response["Status"]
        if status == "FINISHED":
            print(f"Finished executing the following SQL statement: {sql_statement}")
            return
        elif status in ["SUBMITTED", "PICKED", "STARTED"]:
            time.sleep(1)
        elif status == "FAILED":
            print(response)
            raise  ### figure out useful message in exception
        else:
            print(response)
            raise  ### figure out useful message in exception


def lambda_handler(event, context) -> None:
    sql_statements = [
        f'CREATE SCHEMA IF NOT EXISTS "{REDSHIFT_SCHEMA_NAME}";',
        f"""CREATE TABLE IF NOT EXISTS "{REDSHIFT_DATABASE_NAME}"."{REDSHIFT_SCHEMA_NAME}"."{REDSHIFT_TABLE_NAME}" (
            ticker_symbol varchar(4),
            sector varchar(50),
            change float,
            price int
        );""",  # hard coded columns
    ]
    for sql_statement in sql_statements:
        execute_sql_statement(sql_statement=sql_statement)
