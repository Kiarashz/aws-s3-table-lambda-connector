# aws-s3-table-lambda-connector
aws-s3-table-lambda-connector

![image](https://github.com/user-attachments/assets/d94116f7-9836-47d7-a03c-5ac56c4b0fea)

```
export AWS_ACCESS_KEY_ID="XX"
export AWS_SECRET_ACCESS_KEY="X"

docker build -t duckdb-lambda .


docker run -p 9000:8080 \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    duckdb-lambda



curl -s -X POST http://localhost:9000/2015-03-31/functions/function/invocations \
-H "Content-Type: application/json" \
-d '{
    "table_bucket_arn": "XX",
    "namespace": "example_namespace",
    "table": "orders",
    "query": "SELECT * FROM <src> LIMIT 10"
}' | jq .


```
