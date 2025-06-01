Sample event payload:

```
# print(lambda_handler(
#     event={
#         "table_bucket_arn": "",
#         "namespace": "example_namespace",
#         "table": "orders",
#         "query": "SELECT * FROM <src> LIMIT 10"
#     }
# )
# )
```

If running duckdb locally and need config:

```
ATTACH 'arn:aws:s3tables:xxxxxxx' AS s3_tables (
   TYPE iceberg,
   ENDPOINT_TYPE s3_tables
);

SELECT count(*)
FROM s3_tables.tutorial.daily_sales;
```

When using Iceberg:
```
select
   *
from
   iceberg_scan(
      "s3://xxx-table-s3/metadata/xxxxx.metadata.json"
   );
```

If region is not set correctly and wrong endpoint is called by Iceberg/Duckdb you may see IO Error / Wrong HEAD.
```
< Error > < Code > PermanentRedirect < / Code > < Message > The bucket you are attempting to access must be addressed using the specified endpoint.Please send all future requests to this endpoint.< / Message > < Endpoint > b5d19c09 - e16a -4850 - xxxx --table-s3.s3-ap-southeast-2.amazonaws.com</Endpoint>
< Bucket > xxxxx--table-s3</Bucket>
< RequestId > xxxxxx < / RequestId > < HostId > OVe + O + / xxxxxxxx = < / HostId > < / Error >
```