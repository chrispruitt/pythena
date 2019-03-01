import io
import re
from datetime import datetime
from urllib.parse import urlparse
import boto3
import pandas as pd
from retrying import retry


__athena = boto3.client('athena')
__s3 = boto3.client('s3')
__glue = boto3.client('glue')
__s3_path_regex = '^s3:\/\/[a-zA-Z0-9.\-_\/]*$'


class QueryExecutionTimeoutException(Exception):
    pass


class InvalidS3PathException(Exception):
    pass


class QueryExecutionFailedException(Exception):
    pass


class QueryUnknownStatusException(Exception):
    pass


def print_databases():
    result = __glue.get_databases()
    databases = []
    for item in result["DatabaseList"]:
        databases.append(item["Name"])
    __print_list(databases)


def print_tables(database):
    result = __glue.get_tables(DatabaseName=database)
    tables = []
    for item in result["TableList"]:
        tables.append(item["Name"])
    __print_list(tables)


def execute(database, query, s3_output_url=None):
    cleanup_s3_results = False

    if s3_output_url is None:
        s3_output_url = __get_default_s3_url()
        cleanup_s3_results = True

    return __execute_query(database=database,
                           query=query,
                           s3_output_url=s3_output_url,
                           cleanup_s3_results=cleanup_s3_results)


def __execute_query(database, query, s3_output_url, cleanup_s3_results=True):
    s3_bucket, s3_path = __parse_s3_path(s3_output_url)

    response = __athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + s3_bucket + "/" + s3_path,
        })

    query_execution_id = response['QueryExecutionId']
    status = __poll_status(query_execution_id)

    if status == 'SUCCEEDED':
        s3_key = s3_path + "/" + query_execution_id + '.csv'

        obj = __s3.get_object(Bucket=s3_bucket, Key=s3_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))

        # Remove result file from s3
        if cleanup_s3_results:
            __s3.delete_object(Bucket=s3_bucket, Key=s3_key)
            __s3.delete_object(Bucket=s3_bucket, Key=s3_key + '.metadata')

        return df
    elif status == "FAILED":
        raise QueryExecutionFailedException("Query Failed. Check athena logs for more info.")
    else:
        raise QueryUnknownStatusException("Query is in an unknown status. Check athena logs for more info.")


@retry(stop_max_attempt_number=10,
       wait_exponential_multiplier=300,
       wait_exponential_max=60 * 1000)
def __poll_status(query_execution_id):
    res = __athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = res['QueryExecution']['Status']['State']

    if status == 'SUCCEEDED':
        return status
    elif status == 'FAILED':
        return status
    else:
        raise QueryExecutionTimeoutException("Query to athena has timed out. Try running in query in the athena")


# This returns the same bucket and key the AWS Athena console would use for its queries
def __get_default_s3_url():
    account_id = boto3.client('sts').get_caller_identity().get('Account')
    region = boto3.session.Session().region_name
    return 's3://aws-athena-query-results-' + account_id + '-' + region + "/Unsaved/" + datetime.now().strftime("%Y/%m/%d")


def __parse_s3_path(s3_path):
    if not re.compile(__s3_path_regex).match(s3_path):
        raise InvalidS3PathException("s3 Path must follow format: " + __s3_path_regex)
    url = urlparse(s3_path)
    bucket = url.netloc
    path = url.path.lstrip('/')
    return bucket, path


def __print_list(_list):
    _list.sort()
    for item in _list:
        print(item)
