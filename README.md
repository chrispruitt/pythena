# pyathena_simple

This is a simple python module that will allow you to query athena the same way the AWS Athena console would. It only requires a database name and query string.

## Usage

```python
from pyathena_simple import athena

# Returns results as a pandas dataframe
df = athena.execute(database="mydatabase", query="select * from mytable")

print(df.sample(n=2)) # Prints 2 rows from your dataframe
```

Specify an s3 url to save results to a bucket.
```python
from pyathena_simple import athena

# Returns results as a pandas dataframe
df = athena.execute(database="mydatabase", 
                    query="select * from mytable", 
                    s3_output_url="s3://mybucket/mydir")

print(df.sample(n=2)) # Prints 2 rows from your dataframe
```

## Note
By default, when executing athena queries, via boto3 or the AWS athena console, the results are saved in an s3 bucket. This module by default, assuming a successful execution, will delete the s3 result file to keep s3 clean. If an s3_output_url is provided, it the result file will not be deleted.