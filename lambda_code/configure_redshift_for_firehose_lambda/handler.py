import os

import redshift_connector


# aws_redshift.CfnCluster(...).attr_id (for cluster name) is broken, so using endpoint address instead
REDSHIFT_HOST = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(":")[0]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_SCHEMA_NAME_FOR_FIREHOSE = os.environ["REDSHIFT_SCHEMA_NAME_FOR_FIREHOSE"]
REDSHIFT_TABLE_NAME_FOR_FIREHOSE = os.environ["REDSHIFT_TABLE_NAME_FOR_FIREHOSE"]


def lambda_handler(event, context) -> None:
    sql_statements = [
        f'CREATE SCHEMA IF NOT EXISTS "{REDSHIFT_SCHEMA_NAME_FOR_FIREHOSE}";',
        f"""CREATE TABLE IF NOT EXISTS
            "{REDSHIFT_SCHEMA_NAME_FOR_FIREHOSE}"."{REDSHIFT_TABLE_NAME_FOR_FIREHOSE}" (
                card_id varchar,
                amount float,
                merchant_name varchar,
                authorization_type varchar
            );""",  # hard coded columns
    ]
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE_NAME,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
    with conn, conn.cursor() as cursor:
        for sql_statement in sql_statements:
            cursor.execute(sql_statement)
            conn.commit()
            print(f"Finished executing the following SQL statement: {sql_statement}")
