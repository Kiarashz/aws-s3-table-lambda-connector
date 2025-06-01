"""
Apache Iceberg Table Operations with AWS Integration
"""

import boto3
import json
import duckdb
import logging
from datetime import datetime


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Configure logging
logging.basicConfig(
    level=logging.INFO
)  # Change to logging.WARNING to suppress INFO messages

# Constants
REGION = "ap-southeast-2"
TABLE_BUCKET_ARN = (
    "arn:aws:s3tables:xxxxxx"
)
TABLE_NAME = "daily_sales"
NAMESPACE = "tutorial"
QUERY = "SELECT * FROM <src> LIMIT 10"
DUCKDB_EXTENSIONS = [ "aws", "httpfs", "iceberg" ]

def get_table_metadata(table_bucket_arn, namespace, table_name):
    client = boto3.client('s3tables')
    try:
        response = client.get_table(
            tableBucketARN=table_bucket_arn,
            namespace=namespace,
            name=table_name
        )
        return response['metadataLocation']
    except KeyError as e:
        print(f"KeyError: {e}. The response structure might be different.")
        print("Full response:", json.dumps(response, indent=4, cls=DateTimeEncoder))
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise    

def query_iceberg_table_to_df(conn, metadata_location, query):
    try:
        # Replace <src> with Iceberg scan
        iceberg_table = f"iceberg_scan('{metadata_location}')"
        query = query.replace('<src>', iceberg_table)

        # Execute the query and fetch results as a Pandas DataFrame
        print(f"Executing query: {query}")
        df = conn.execute(query).fetchdf()
        return df
    except Exception as e:
        print(f"An error occurred during query execution: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def lambda_handler(event, context=None):
    try:
        table_bucket_arn = event['table_bucket_arn']
        namespace = event['namespace']
        table = event['table']
        query = event['query']

        # Get metadata location
        metadata_location = get_table_metadata(table_bucket_arn, namespace, table)
        print("Metadata location:", metadata_location)

        # Query Iceberg table and return the DataFrame as JSON
        df = query_iceberg_table_to_df(metadata_location, query)

        result = df.to_json(orient='records', date_format='iso')
        return {
            "statusCode": 200,
            "body": result
        }
    except Exception as e:
        print(f"An error occurred in the Lambda handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def get_aws_account_id():
    """Attempt to get account ID from STS."""
    try:
        sts_client = boto3.client("sts")
        account_id = sts_client.get_caller_identity().get("Account")
        return account_id
    except Exception as e:
        print(f"Error getting account ID: {e}")
        return None


def main():
    """Main function to orchestrate Iceberg table operations."""
    account_id = get_aws_account_id()
    if not account_id:
        exit()

    try:
        # Connect to DuckDB in-memory database
        con = duckdb.connect(database=":memory:")

        # Install and load the necessary extensions
        for ext in DUCKDB_EXTENSIONS:
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
            logging.info(f"Installed and loaded extension: {ext}")

        # Set S3 endpoint and retry configurations
        con.execute(f"SET s3_endpoint='s3.{REGION}.amazonaws.com';")
        con.execute("SET http_retries = 10;")
        con.execute("SET http_retry_backoff = 5;")

        # Load AWS credentials (if necessary)
        con.execute("CALL load_aws_credentials();")

        # Get metadata location
        metadata_location = get_table_metadata(TABLE_BUCKET_ARN, NAMESPACE, TABLE_NAME)
        print("Metadata location:", metadata_location)

        # Query Iceberg table and return the DataFrame as JSON
        df = query_iceberg_table_to_df(con, metadata_location, QUERY)

        result = df.to_json(orient="records", date_format="iso")
        print("Query result:", result)

    except Exception as e:
        print(f"An error occurred in the Lambda handler: {e}")
        exit(1)

if __name__ == "__main__":
    main()
