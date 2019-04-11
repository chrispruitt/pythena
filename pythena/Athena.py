import io
import re
from datetime import datetime
from urllib.parse import urlparse
import boto3
import pandas as pd
from retrying import retry
import pythena.Utils as Utils
import pythena.Exceptions as Exceptions


class Athena:

    __database = ''
    __region = ''
    __athena = None
    __s3 = None
    __glue = None
    __s3_path_regex = '^s3:\/\/[a-zA-Z0-9.\-_\/]*$'

    def __init__(self, database, region='us-east-1'):
        self.__database = database
        self.__region = region
        if region is None:
            region = boto3.session.Session().region_name
            if region is None:
                raise Exceptions.NoRegionFoundError("No default aws region configuration found. Must specify a region.")
        self.__athena = boto3.client('athena', region_name=region)
        self.__s3 = boto3.client('s3', region_name=region)
        self.__glue = boto3.client('glue', region_name=region)
        if database not in Utils.get_databases(region):
            raise Exceptions.DatabaseNotFound("Database " + database + " not found.")

    def get_tables(self):
        result = self.__glue.get_tables(DatabaseName=self.__database)
        tables = []
        for item in result["TableList"]:
            tables.append(item["Name"])
        return tables

    def print_tables(self):
        Utils.print_list(self.get_tables())

    def execute(self, query, s3_output_url=None, save_results=False):

        if s3_output_url is None:
            s3_output_url = self.__get_default_s3_url()
        else:
            save_results = True

        return self.__execute_query(database=self.__database,
                                    query=query,
                                    s3_output_url=s3_output_url,
                                    save_results=save_results)

    def __execute_query(self, database, query, s3_output_url, return_results=True, save_results=True):
        s3_bucket, s3_path = self.__parse_s3_path(s3_output_url)

        response = self.__athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://' + s3_bucket + "/" + s3_path,
            })

        query_execution_id = response['QueryExecutionId']
        status = self.__poll_status(query_execution_id)

        if status == 'SUCCEEDED':
            s3_key = s3_path + "/" + query_execution_id + '.csv'

            if return_results:
                obj = self.__s3.get_object(Bucket=s3_bucket, Key=s3_key)
                df = pd.read_csv(io.BytesIO(obj['Body'].read()))

                # Remove result file from s3
                if not save_results:
                    self.__s3.delete_object(Bucket=s3_bucket, Key=s3_key)
                    self.__s3.delete_object(Bucket=s3_bucket, Key=s3_key + '.metadata')

                return df, query_execution_id
            else:
                return query_execution_id
        elif status == "FAILED":
            raise Exceptions.QueryExecutionFailedException("Query Failed. Check athena logs for more info.")
        else:
            raise Exceptions.QueryUnknownStatusException("Query is in an unknown status. Check athena logs for more info.")

    @retry(stop_max_attempt_number=10,
           wait_exponential_multiplier=300,
           wait_exponential_max=60 * 1000)
    def __poll_status(self, query_execution_id):
        res = self.__athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = res['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            return status
        elif status == 'FAILED':
            return status
        else:
            raise Exceptions.QueryExecutionTimeoutException("Query to athena has timed out. Try running in query in the athena")

    # This returns the same bucket and key the AWS Athena console would use for its queries
    def __get_default_s3_url(self):
        account_id = boto3.client('sts').get_caller_identity().get('Account')
        return 's3://aws-athena-query-results-' + account_id + '-' + self.__region + "/Unsaved/" + datetime.now().strftime("%Y/%m/%d")

    def __parse_s3_path(self, s3_path):
        if not re.compile(self.__s3_path_regex).match(s3_path):
            raise Exceptions.InvalidS3PathException("s3 Path must follow format: " + self.__s3_path_regex)
        url = urlparse(s3_path)
        bucket = url.netloc
        path = url.path.lstrip('/')
        return bucket, path

