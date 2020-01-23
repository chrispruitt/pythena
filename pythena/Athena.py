import io
import re
from datetime import datetime
from urllib.parse import urlparse
import boto3
import pandas as pd
from retrying import retry
import pythena.Utils as Utils
import pythena.Exceptions as Exceptions
from botocore.errorfactory import ClientError
import logging
logger = logging.getLogger(__name__)


class Athena:

    __database = ''
    __region = ''
    _athena = None
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
        self._athena = boto3.client('athena', region_name=region)
        self.__s3 = boto3.client('s3', region_name=region)
        self.__glue = boto3.client('glue', region_name=region)
        if database not in Utils.get_databases(region):
            raise Exceptions.DatabaseNotFound("Database " + database + " not found.")

    def get_tables(self):
        tables = []
        params = {
            "DatabaseName": self.__database
        }

        while True:
            result = self.__glue.get_tables(**params)

            for item in result["TableList"]:
                tables.append(item["Name"])
            if result.get('NextToken') is None:
                break
            else:
                params['NextToken'] = result.get('NextToken')

        return tables

    def get_columns(self, table_name):
        table = self.__glue.get_table(DatabaseName=self.__database, Name=table_name)['Table']
        cols = table['StorageDescriptor']['Columns']
        cols = [c['Name'] for c in cols]
        return cols

    def get_all_running_queries(self):
        exe_ids = self._athena.list_query_executions()['QueryExecutionIds']
        running_ids = [i for i in exe_ids if self.get_query_status(i) == "RUNNING"]
        return running_ids

    def stop_all_queries(self):
        running_ids = self.get_all_running_queries()
        for ids in running_ids:
            self._athena.stop_query_execution(QueryExecutionId=ids)
        logger.info(f"Stopped {len(running_ids)} queries")

    def print_tables(self):
        Utils.print_list(self.get_tables())

    def execute(self, query, s3_output_url=None, save_results=False, run_async=False):
        '''
        Execute a query on Athena
        -- If run_async is false, returns dataframe and query id. If true, returns just the query id
        -- Data deleted unless save_results true, to keep s3 bucket clean
        -- Uses default s3 output url unless otherwise specified 
        '''

        if s3_output_url is None:
            s3_output_url = self.__get_default_s3_url()
        else:
            save_results = True

        return self.__execute_query(database=self.__database,
                                    query=query,
                                    s3_output_url=s3_output_url,
                                    save_results=save_results,
                                    run_async=run_async)

    def __execute_query(self, database, query, s3_output_url, return_results=True, save_results=True, run_async=False):
        s3_bucket, s3_path = self.__parse_s3_path(s3_output_url)

        response = self._athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://' + s3_bucket + "/" + s3_path,
            })

        query_execution_id = response['QueryExecutionId']

        # If executing asynchronously, just return the id so results can be fetched later. Else, return dataframe (or error message)
        if run_async: 
          return query_execution_id
        else:
            status = self.__poll_status(query_execution_id)
            df = self.get_result(query_execution_id)
            return df, query_execution_id

    def get_result(self, query_execution_id, save_results=False):
        '''
        Given an execution id, returns result as a pandas df if successful. Prints error otherwise. 
        -- Data deleted unless save_results true
        '''
        # Get execution status and save path, which we can then split into bucket and key. Automatically handles csv/txt 
        res = self._athena.get_query_execution(QueryExecutionId = query_execution_id)
        s3_bucket, s3_key = self.__parse_s3_path(res['QueryExecution']['ResultConfiguration']['OutputLocation'])

        # If succeed, return df
        if res['QueryExecution']['Status']['State'] == 'SUCCEEDED':
            obj = self.__s3.get_object(Bucket=s3_bucket, Key=s3_key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            
            # Remove results from s3
            if not save_results:
                self.__s3.delete_object(Bucket=s3_bucket, Key=s3_key)
                self.__s3.delete_object(Bucket=s3_bucket, Key=s3_key + '.metadata')

            return df

        # If failed, return error message
        elif res['QueryExecution']['Status']['State'] == 'FAILED':
            raise Exceptions.QueryExecutionFailedException("Query failed with response: %s" % (self.get_query_error(query_execution_id)))
        elif res['QueryExecution']['Status']['State'] == 'RUNNING':
            raise Exceptions.QueryStillRunningException("Query has not finished executing.")
        else: 
            raise Exceptions.QueryUnknownStatusException("Query is in an unknown status. Check athena logs for more info.")


    @retry(stop_max_attempt_number=10,
           wait_exponential_multiplier=300,
           wait_exponential_max=60 * 1000)
    def __poll_status(self, query_execution_id):
        status = self.get_query_status(query_execution_id)
        if status in ['SUCCEEDED', 'FAILED']:
            return status
        else:
            raise Exceptions.QueryExecutionTimeoutException("Query to athena has timed out. Try running the query in the athena or asynchronously")

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
    
    # A few functions to surface boto client functions to pythena: get status, get query error, and cancel a query
    def get_query_status(self, query_execution_id):
        res = self._athena.get_query_execution(QueryExecutionId=query_execution_id)
        return res['QueryExecution']['Status']['State']

    def get_query_error(self, query_execution_id):
        res = self._athena.get_query_execution(QueryExecutionId=query_execution_id)
        if res['QueryExecution']['Status']['State']=='FAILED':
            return res['QueryExecution']['Status']['StateChangeReason']
        else: 
            return "Query has not failed: check status or see Athena log for more details"

    def cancel_query(self, query_execution_id): 
        self._athena.stop_query_execution(QueryExecutionId=query_execution_id)
        return 




