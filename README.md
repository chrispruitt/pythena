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

## Full Usage

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
# Connect to a database and override default aws region in your aws configuration
athena_client = pythena.Athena(database="mydatabase", region='us-east-1')

# Prints out all tables in a database
athena_client.print_tables()

# Gets all tables in the database you are connected to and returns as a list
athena_client.get_tables()

# Execute a query
dataframe = athena_client.execute(query="select * from my_table") # Results are  returned as a dataframe

# Execute a query and save results to s3
dataframe = athena_client.execute(query="select * from my_table", s3_output_url="s3://mybucket/mydir") # Results are  returned as a dataframe

```

## Note
By default, when executing athena queries, via boto3 or the AWS athena console, the results are saved in an s3 bucket. This module by default, assuming a successful execution, will delete the s3 result file to keep s3 clean. If an s3_output_url is provided, then the results will be saved to that location and will not be deleted.