import boto3
import json
from datetime import datetime
import duckdb
import pandas as pd


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


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


def query_iceberg_table_to_df(metadata_location, query):
    try:
        # Connect to DuckDB
        conn = duckdb.connect(':memory:')  # Create in-memory DuckDB database
        conn.execute("INSTALL aws; LOAD aws;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("CALL load_aws_credentials();")

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

# print(lambda_handler(
#     event={
#         "table_bucket_arn": "",
#         "namespace": "example_namespace",
#         "table": "orders",
#         "query": "SELECT * FROM <src> LIMIT 10"
#     }
# )
# )
