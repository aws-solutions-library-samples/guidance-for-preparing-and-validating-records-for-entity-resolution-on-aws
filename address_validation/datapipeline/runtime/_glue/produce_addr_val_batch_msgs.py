"""
AWS lambda code that reads S3 data files and generates SQS messages in batches of 10 max
Uses awswrangler python module to read S3 data files
"""
import boto3
import json
import awswrangler as wr
import logging
from botocore.exceptions import ClientError
# from util import utils # use this to run as lambda function
import utils # use this only for glue jobs
import os
import sys
from awsglue.utils import getResolvedOptions # use this only for glue jobs

# set logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# reads S3 file in csv format using awswrangler
# takes four inputs : bucket, key, chunk_size, delimiter, encoding, limit_rows
# uses na_values=['null', 'none']
def read_s3_file_chunked(bucket, key, chunk_size=100, delimiter=",", encoding="utf-8", limit_rows=1000, cli_profile=None) -> iter:
    session = boto3.session.Session(profile_name=cli_profile)
    df = wr.s3.read_csv(path=f"s3://{bucket}/{key}",
                        chunksize=chunk_size,
                        delimiter=delimiter,
                        encoding=encoding,
                        nrows=limit_rows,
                        na_values=['null', 'none'],
                        boto3_session=session)
    return df

def send_message(queue, message_body, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}
    logger.debug(message_body)
    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response

# sends the batch message list to SQS
def send_msg_list_to_sqs(msg_list: list, sqs_queue_url:str):
    sqs = boto3.resource('sqs')
    queue = sqs.Queue(sqs_queue_url)
    for msg in msg_list:
        response = send_message(queue, msg)
        logger.info("**Response from SQS**")
        logger.info(response)

def read_and_produce_df_chunk(df_iterator: iter, sqs_queue_url:str):

    sqs = boto3.resource('sqs')
    queue = sqs.Queue(sqs_queue_url)
    for index, df_chunk in enumerate(df_iterator):
        logger.info(f"processing chunk {index}")
        logger.info(f"chunk row count: {len(df_chunk.index)}")

        if (df_chunk.empty):
            logger.info("***************")
            logger.error("Empty dataframe detected. Exiting")
            logger.info("***************")
            exit(2)
        
        # calculate the total message size 
        total_msg_size = len(json.dumps(df_chunk.to_dict("records")))
        logger.info(f"total_msg_size after adding the current chunk: {total_msg_size}")
        
        if (total_msg_size > 256000):
            logger.info("***************")
            logger.info("Size greater than 256kb. Exiting")
            logger.info("***************")
            exit(2)
        
        msg_batch = json.dumps(df_chunk.to_dict("records"))
        # send the message
        send_message(queue, msg_batch)
        logger.info("**Send message**")
        #logger.info(msg_batch)

def read_and_produce_max_size(df_iterator: iter, sqs_queue_url:str):

    total_msg_size = 0
    msg_list = []
    msg_count = 0
    sqs = boto3.resource('sqs')
    queue = sqs.Queue(sqs_queue_url)
    for index, df_chunk in enumerate(df_iterator):
        logger.info(f"processing chunk {index}")
        logger.info(f"chunk row count: {len(df_chunk.index)}")

        if (df_chunk.empty):
            logger.info("***************")
            logger.error("Empty dataframe detected. Exiting")
            logger.info("***************")
            exit(2)
        
        # calculate the total message size when we add the current chunk to the message list
        prev_msg_size = total_msg_size
        total_msg_size += len(json.dumps(df_chunk.to_dict("records")))
        logger.info(f"total_msg_size after adding the current chunk: {total_msg_size}")
        
        # increment the message count
        prev_msg_count = msg_count
        msg_count += 1

        # if the size of the message with the new addition is greater than or equal to 256000 bytes send the message without appending
        # else append the current row to the message list
        if total_msg_size >= 256000:
            logger.info("**total_msg_size > 256000**")
            # build the message body
            msg_batch = json.dumps(msg_list)
            # send the message
            send_message(queue, msg_batch)
            logger.info("**Sent message**")
            #logger.info(msg_batch)
            logger.info(f"total chunks in this send {prev_msg_count}")
            logger.info(f"total message size in this send {prev_msg_size}")
            msg_list = []
            total_msg_size = 0
            msg_count = 0

        else:
            logger.info("adding to msg_list")
            msg_list.extend(df_chunk.to_dict("records"))

    # when the loop exits msg_list has the remaining messages
    # build the message body
    msg_batch = json.dumps(msg_list)
    # send the message
    send_message(queue, msg_batch)
    logger.info("**Final Message Sent**")
    #logger.info(msg_batch)
    logger.info(f"total chunks in this send {msg_count}")
    logger.info(f"total message size in this send {total_msg_size}")

# if the input is an s3 object create event then extract the file information from the event and process
def lambda_handler(event, context):

    bucket,key, full_uri = utils.get_s3_bucket_and_key(event)

    # read rest of the configuration for SSM parameter store
    ssm_parameter = os.environ['SSM_PARAMETER']
    # use named profile for local testing, in lamda env this will be(and has to be) None
    try:
        cli_profile = os.environ['CLI_PROFILE']
    except(KeyError):
        cli_profile = None
        
    config_json = utils.get_app_configuration(ssm_parameter,cli_profile=cli_profile)
    chunk_size = config_json['chunk_size']
    delimiter = config_json['delimiter']
    encoding = config_json['encoding']
    limit_rows = config_json['limit_rows']
    cli_profile = config_json['cli_profile']
    sqs_queue = config_json['sqs_queue']
    batch_mode = config_json["batch_mode"]
    df_chunk = read_s3_file_chunked(bucket, key, chunk_size, delimiter, encoding, limit_rows, cli_profile)
    
    if batch_mode == "chunk_size":
        read_and_produce_df_chunk(df_chunk, sqs_queue)
    elif batch_mode == "max_size":
        read_and_produce_max_size(df_chunk, sqs_queue)
    else:
        logger.error("**Error invalid batch_mode. Valid values chunk_size|max_size**")
        exit(3)

# lambda handler that reads amazon step function input and calls the read_and_produce_df_chunk function
# rename this to lambda_handler to use it as a step function triggered lambda and give payload input json 
# to the stepfunction
def lambda_handler_sfn(event, context):
    # use named profile for local testing, in lamda env this will be(and has to be) None
    try:
        cli_profile = os.environ['CLI_PROFILE']
    except(KeyError):
        cli_profile = None
    bucket = event['bucket']
    key = event['key']
    chunk_size = int(event['chunk_size'])
    delimiter = event['delimiter']
    encoding = event['encoding']
    limit_rows = int(event['limit_rows'])
    sqs_queue = event['sqs_queue']
    batch_mode = event["batch_mode"]
    df_chunk = read_s3_file_chunked(bucket, key, chunk_size, delimiter, encoding, limit_rows, cli_profile)
    
    if batch_mode == "chunk_size":
        read_and_produce_df_chunk(df_chunk, sqs_queue)
    elif batch_mode == "max_size":
        read_and_produce_max_size(df_chunk, sqs_queue)
    else:
        logger.info("**Error invalid batch_mode. Valid values chunk_size|max_size**")
    
# use this for local testing through cli or shell execution
if __name__ == "__main__":
    # use below for executing as a glue python shell job
    args = getResolvedOptions(sys.argv,
                                ['bucket',
                                'key',
                                'chunk_size',
                                'delimiter',
                                'encoding',
                                'limit_rows',
                                'sqs_queue',
                                'batch_mode'])
    print(args)
    lambda_handler_sfn(args, None)
    # lambda_handler(get_sample_sfn_input_config(), None)

    # use below to test the s3 file upload event trigger based execution in lambda
    # lambda_handler(utils.get_s3_sample_event(), None)