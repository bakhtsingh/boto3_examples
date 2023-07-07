import boto3
import io
import pandas as pd
import time

from exam import get_string

def run_query(query_string: str, query_result_location: str):
    """
    Executes an Athena query and returns the results from s3.
    Parameters:
    query_string: the query to be executed (as a string)
    query_result_location: the s3 location where Athena saves the results (as a string)
    """
    AWS_ACCESS_KEY = "XXXXXXXXXX"
    AWS_SECRET_KEY = "XXXXXXXXXX"
    AWS_REGION = "us-east-1"
    #initialize "athena" service
    athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION)
    #initialize "s3" service
    S3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    response = athena_client.start_query_execution(QueryString=query_string)
    query_execution_id = response['QueryExecutionId']

    wait_for_query_completion(query_execution_id, S3_client)

    query_result_file_name = query_result_location + query_execution_id + '.csv'

    bucket = query_result_file_name.replace('s3://').split('/', 1)[0]
    key = query_result_file_name.replace('s3://').split('/', 1)[-1]

    obj = S3_client.get_object(Bucket=bucket, Key=key)
    result_df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    return result_df

def wait_for_query_completion(query_execution_id: str, s3):
    """
    determines if the query has finished excuting. If it has not, the function waits for the query to finished.
    Parameters:
    query_execution_id: the query execution id (as a string)
    """
    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
            max_execution = max_execution - 1
            response = s3.get_query_execution(QueryExecutionId = query_execution_id)

            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']
                if state == 'FAILED':
                    return False
                elif state == 'SUCCEEDED':
                    s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    filename = response.findall('.*\/(.*)', s3_path)[0]
                    return filename
            time.sleep(1)
    return False

def query_string(table_name,type1,tagcategory,dict_tags,locality_dict):
    string = """
                SELECT * from {table_name} WHERE type = '{type1}' AND tags['{tagcategory}']
                IN {dictionaryOfTags}
                AND lon BETWEEN {minLongitude} AND {maxLongitude}
                AND lat BETWEEN {minLatitude} AND {maxLatitude}  """
    
    string.format(table_name=table_name, type1=type1, tagcategory=tagcategory,dictionaryOfTags = dict_tags,\
                       minLongitude=locality_dict['local'][0], maxLongitude= locality_dict['local'][1], \
                       minLatitude=locality_dict['local'][2], maxLatitude=locality_dict['local'][3])
    return query_string
example_dict = {'places':"('city', 'town', 'village', 'hamlet')",'amenity':"('place_of_worship')"}
dict_tags = example_dict['amenity']
locality_dict={'local':(-103.002565, -94.430662, 33.615833, 37.002206)}
query = query_string("planet","node","amenity",dict_tags, locality_dict)
print(query)

def get_string(length):
    queryin = ["('{dictionaryOfTags}')","{dictionaryOfTags}"]
    if length > 1:
        string = """
                    SELECT * from {table_name} WHERE type = '{type1}' AND tags['{tagcategory}']
                    IN """+queryin[1]+"""
                    AND lon BETWEEN {minLongitude} AND {maxLongitude}
                    AND lat BETWEEN {minLatitude} AND {maxLatitude}  """
    else:
        string = """
                    SELECT * from {table_name} WHERE type = '{type1}' AND tags['{tagcategory}']
                    IN """+queryin[0]+"""
                    AND lon BETWEEN {minLongitude} AND {maxLongitude}
                    AND lat BETWEEN {minLatitude} AND {maxLatitude}  """
    return string
                    
