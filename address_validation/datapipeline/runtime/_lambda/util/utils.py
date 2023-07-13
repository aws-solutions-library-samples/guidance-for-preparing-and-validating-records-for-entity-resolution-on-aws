import requests
import json
import os
import boto3
import logging
from botocore.exceptions import ClientError

# set logging
logger = logging.getLogger()

# get sample configuration json
def get_sample_configuration() -> dict:
    """
    Gets sample configuration dictionary
    """
    config_dict = {
        "license_key": "us-core-cloud",
        "url": "https://us-street.api.smartystreets.com/street-address",
        "secret_name":"dev/id-res/addr-val/smartyapi",
        "region_name": "us-west-2",
        "s3_bucket": "id-res-addr-val-smartyapi",
        "s3_key": "test-drive/synthetic_testData.csv",
    }
    return config_dict

# make http post request to url with data, use this when your validation service do not have an python SDK
def make_http_post_request(url, data):
    """
    Makes http post request to url with data
    """
    headers = {'Content-Type': 'application/json', 'charset': 'utf-8'}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    logger.info(response.status_code)
    logger.info(response.text)
    return response

# function to generate sample physical address json
def get_sample_physical_address() -> dict:
    """
    Generates sample physical address json
    """
    physical_address = {
        "address1": "123 Main St",
        "city": "San Francisco",
        "state_code": "CA",
        "zip_code": "94105",
        "input_id": "abcxyz123"
    }
    return physical_address

# sample address record string
def sample_address_record_str():
    return '123 southwest Main street, unit 2, Boring, or, 97203'

# take a dict with address data and another dict with schema mapping and return a concatenated string of address data
def get_address_data_string(address_data: dict, schema_mapping: dict) -> str:
    """
    Returns a concatenated string of address data
    """
    address_data_string = ""
    for key, value in schema_mapping.items():
        if value in address_data.keys():
            # print(key,value)
            address_data_string += f"{address_data[value]} "
    # print(address_data_string)
    # TODO need a more elegant NaN cleanup solution
    return address_data_string.replace("nan", "")

# return a sample address schema mapping
def get_sample_address_schema_mapping() -> dict:
    """
    Returns a sample address schema mapping
    """
    schema_mapping = {
        "address_line1": "address1",
        "address_line2": "address2",
        "city": "city",
        "state": "state_code",
        "zip_code": "zip_code",
        "country": "country"
    }
    return schema_mapping

# function to generate a sample sqs message input
def get_sample_sqs_message_input() -> dict:
    """
    Generates a sample sqs message input
    """
    message_input = {
        "Records": [
            {
                "messageId": "fb38149e-95b5-4b66-846c-4a92951c03b7",
                "receiptHandle": "AQEBl+HSHNIqWzGXllRJkuxonhPF7hpeOJCd2z8+R29WUQaiUr8Ecn3z2gdzj750qwX9bJipHekWplO/SU2pYkSq6L0futiFdxjaSqcrFY+wTUYslCWxjzf2gnXNBhkMECEj/9wjY/wNvThRt1Fz0SSvhEer297hpUzsg8N48nJDLucB2EBCMynJ0PstBTtlxeR7QPDiWRUmeNUmkMRrouyaVICrc+h2XEIAr2WZ8US3wkzpg7Q30l/yLQ5Wn6mScghXhp0/7fzfEXe1piDUj/vUz+RRTxxuuGZX8N2Dsg1RLECBQqchk1hn1dwciiK2UXQIiBOhUvvyBJFxnwdZNAfjgC1cIPa46BpWLI0jhHxXPIvt5Owt/mx1hr9dPvRTlfwhZ/1RiUuLz80tFeDo6i2LeQ==",
                "body": "[{\"source_id\": \"bd530c1554874f1eadb2315ae8a9f3d1\", \"last_name\": \"Davis\", \"first_name\": \"Andrea\", \"middle_initial\": NaN, \"suffix_name\": NaN, \"zip_code\": 94105, \"country\": \"UNITED STATES\", \"country_code\": \"US\", \"address1\": \"123 Main St\", \"address2\": NaN, \"address3\": NaN, \"address4\": NaN, \"city\": \"San Francisco\", \"state_code\": \"California\", \"state_name\": \"CA\", \"email_address\": \"jamie87@example.net\", \"continent\": \"NORTH AMERICA\", \"name_prefix\": NaN, \"phone_nbr\": \"883.320.5019x8778\", \"phone_ext\": NaN, \"phone_type\": \"HOME\"}, {\"source_id\": \"d1e5ce41759e4378b9b117879c4096df\", \"last_name\": \"Adams\", \"first_name\": \"David\", \"middle_initial\": NaN, \"suffix_name\": NaN, \"zip_code\": 64720, \"country\": \"UNITED STATES\", \"country_code\": \"US\", \"address1\": \"9667 Holly Flat Apt. 998\", \"address2\": NaN, \"address3\": NaN, \"address4\": NaN, \"city\": \"Cindyland\", \"state_code\": \"North Carolina\", \"state_name\": \"IN\", \"email_address\": \"jonesgina@example.org\", \"continent\": \"NORTH AMERICA\", \"name_prefix\": NaN, \"phone_nbr\": \"065-668-6338x4961\", \"phone_ext\": NaN, \"phone_type\": \"HOME\"}]",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1685123449186",
                    "SenderId": "AROAUPRF2AVSRXVL64QKV:GlueJobRunnerSession",
                    "ApproximateFirstReceiveTimestamp": "1685123449187"
                },
                "messageAttributes": {},
                "md5OfBody": "9d03e86562a85c04a91d112044fcf92f",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:xxxxxxxxxxxx:smarty-addr-val-request",
                "awsRegion": "us-west-2"
            }
        ]
    }
    return message_input

# function to generate sample physical address batch list
def get_sample_physical_address_batch_list() -> list:
    """
    Generates sample physical address batch list
    """
    physical_address_batch_list = [
        get_sample_physical_address(),
        get_sample_physical_address(),
        get_sample_physical_address()
    ]
    return physical_address_batch_list

# function to retrieve secret credentials from AWS Secrets Manager
def get_secret_credentials(secret_name: str, region_name: str, cli_profile=None) -> dict:
    """
    Retrieves secret credentials from AWS Secrets Manager
    """
    logger.info(f"trying to fetch secret credentials from AWS Secrets Manager: {secret_name} from region {region_name}")
    # if profile is given build a client using that profile else use default profile
    logger.info(f"using profile {cli_profile}")
    session = boto3.session.Session(profile_name=cli_profile)
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    
    secret = get_secret_value_response['SecretString']
    secret_json = json.loads(secret)
    return secret_json

# function to retrieve application configuration from AWS SSM Parameter Store
def get_app_configuration(param_name: str, cli_profile: str=None) -> dict:
    """
    Retrieves application configuration from AWS SSM Parameter Store
    """
    # if profile is given build a client using that profile else use default profile
    session = boto3.session.Session(profile_name=cli_profile)
    if cli_profile:
        ssm_client = session.client('ssm')
    else: 
        ssm_client = boto3.client('ssm')

    # check if param_name is given, if not use SSM_PARAMETER_NAME environment variable
    if param_name:
        ssm_parameter_name = param_name
    else:
        ssm_parameter_name = os.environ['SSM_PARAMETER_NAME']

    ssm_parameter_value = ssm_client.get_parameter(Name=ssm_parameter_name)
    ssm_parameter_value_json = json.loads(ssm_parameter_value['Parameter']['Value'])
    return ssm_parameter_value_json

# add the secrets configuration to the config
def add_secrets_to_config(config: dict, cli_profile: str=None) -> dict:
    """
    Adds the secrets configuration to the config
    """
    # retrieve secret credentials from AWS Secrets Manager
    secret_credentials = get_secret_credentials(config['secret_name'], config['region_name'], cli_profile)
    # add the secret credentials to the config
    config['secrets'] = secret_credentials
    return config

# build address validation smarty url with configuration json as input
def build_address_validation_smarty_url(configuration_json) -> str:
    """
    Builds address validation smarty url with configuration json as input
    """
    url = f'{configuration_json["url"]}?auth-id={configuration_json["auth_id"]}&auth-token={configuration_json["auth_token"]}&license={configuration_json["license_key"]}'
    return url

# read from SQS queue and create batch of physical addresses. use this if you are using a scheduled glue python shell job or testing
def read_from_sqs_queue(sqs_queue_url: str, batch_size: int) -> list:
    """
    Reads from SQS queue and create batch of physical addresses
    """
    sqs_client = boto3.client('sqs')
    sqs_response = sqs_client.receive_message(
        QueueUrl=sqs_queue_url,
        MaxNumberOfMessages=batch_size,
        WaitTimeSeconds=20
    )
    message_list = sqs_response.get('Messages', [])
    return message_list

# read the msg_batch of 256k, split it in to batches of 100 rows
def split_msg_batch_into_batches(msg_batch: list, batch_size: int) -> list:
    """
    Splits the msg_batch of 256k into batches of 100 rows
    """
    # create a list of batches of batch_size rows
    batch_list = []
    for i in range(0, len(msg_batch), batch_size):
        batch_list.append(msg_batch[i:i+batch_size])
    return batch_list

# take the s3 lambda file create event as input and return the s3 bucket and key
def get_s3_bucket_and_key(event: dict) -> tuple:
    """
    Gets the s3 bucket and key from the s3 lambda file create event
    """
    bucket = event['detail']['bucket']['name']
    folder_path = event['detail']['object']['key']
    full_file_uri = f's3://{bucket}/{folder_path}'
    return(bucket, folder_path, full_file_uri)

# sample event for s3 object create
def get_s3_sample_event() -> dict:
    """
    returns sample payload for testing purposes
    """
    payload = {
        "version": "0",
        "id": "2d4eba74-fd51-3966-4bfa-b013c9da8ff1",
        "detail-type": "Object Created",
        "source": "aws.s3",
        "account": "123456789012",
        "time": "2021-11-13T00:00:59Z",
        "region": "us-west-2",
        "resources": [
            "<>"
        ],
        "detail": {
            "version": "0",
            "bucket": {
            "name": "idres-test-input-data"
            },
            "object": {
            "key": "test-drive/synthetic_testData.csv",
            "size": 99797,
            "etag": "7a72374e1238761aca7778318b363232",
            "version-id": "a7diKodKIlW3mHIvhGvVphz5N_ZcL3RG",
            "sequencer": "00618F003B7286F496"
            },
            "request-id": "4Z2S00BKW2P1AQK8",
            "requester": "348414629041",
            "source-ip-address": "72.21.198.68",
            "reason": "PutObject"
        }
    }
    return payload

# sample stepfunction input config
def get_sample_sfn_input_config() -> dict:
    """
    returns sample stepfunction input config
    """
    config = {
        "bucket": "idres-test-input-data",
        "key": "test-drive/synthetic_testData.csv",
        "chunk_size": "100",
        "delimiter": ",",
        "encoding": "utf-8",
        "limit_rows": "1000",
        "cli_profile": "cleanroom1",
        "sqs_queue": "https://sqs.us-west-2.amazonaws.com/308243203429/smarty-addr-val-request",
        "batch_mode": "chunk_size"
    }
    return config

