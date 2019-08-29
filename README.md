# pythena

This is a simple python module that will allow you to query athena the same way the AWS Athena console would. It only requires a database name and query string.

## Install
```bash
pip install pythena
```

## Setup
Be sure to set up your AWS authentication credentials. You can do so by using the aws cli and running
```
pip install awscli
aws configure
```
More help on configuring the aws cli here https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html


## Simple Usage

```python
import pythena

athena_client = pythena.Athena("mydatabase") 

# Returns results as a pandas dataframe
df = athena_client.execute("select * from mytable")

print(df.sample(n=2)) # Prints 2 rows from your dataframe
```

## Connect to Database
```python
import pythena

# Connect to a database
athena_client = pythena.Athena(database="mydatabase")
# Connect to a database and override default aws region in your aws configuration
athena_client = pythena.Athena(database="mydatabase", region='us-east-1')

```

## athena_client.execute()
```
execute(
  query = 'SQL_QUERY',                 # Required
  s3_output_url='FULL_S3_PATH',        # Optional (Format example: 's3://mybucket/mydir'
  save_results=TRUE | FALSE            # Optional. Defaults to True only when 's3_output_url' is provided. If True, the s3 results will not be deleted and an tuple is returned with the execution_id.
  run_async=TRUE | FALSE               # Optional. If True, allows you to run the query asynchronously. Returns execution_id, use get_result(execution_id) to fetch it when finished
)
```

## Full Usage Examples

```python
import pythena

# Prints out all databases listed in the glue catalog
pythena.print_databases()
pythena.print_databases(region='us-east-1') # Overrides default region

# Gets all databases and returns as a list
pythena.get_databases()
pythena.get_databases(region='us-east-1') # Overrides default region

# Connect to a database
athena_client = pythena.Athena(database="mydatabase")

# Prints out all tables in a database
athena_client.print_tables()

# Gets all tables in the database you are connected to and returns as a list
athena_client.get_tables()

# Execute a query
dataframe = athena_client.execute(query="select * from my_table") # Results are  returned as a dataframe

# Execute a query and save results to s3
dataframe = athena_client.execute(query="select * from my_table", s3_output_url="s3://mybucket/mydir") # Results are  returned as a dataframe

# Get Execution Id and save results
dataframe, execution_id = athena_client.execute(query="select * from my_table", save_results=True)

# Execute a query asynchronously
execution_id = athena_client.execute(query="select * from my_table", run_async=True) # Returns just the execution id 
dataframe = athena_client.get_result(execution_id) # Will report errors if query failed or let you know if it is still running

# With asynchronous queries, can check status, get error, or cancel
pythena.get_query_status(execution_id)
pythena.get_query_error(execution_id)
pythena.cancel_query(execution_id)

```

## Note
By default, when executing athena queries, via boto3 or the AWS athena console, the results are saved in an s3 bucket. This module by default, assuming a successful execution, will delete the s3 result file to keep s3 clean. If an s3_output_url is provided, then the results will be saved to that location and will not be deleted.
