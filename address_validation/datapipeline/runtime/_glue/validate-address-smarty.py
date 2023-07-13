"""
AWS glue python shell code that reads S3 data files and generates SQS messages in batches of 10 max
Uses awswrangler python module to read S3 data files
"""
import boto3
import json
import awswrangler as wr
import logging
from botocore.exceptions import ClientError
import utils
import os
import sys
from awsglue.utils import getResolvedOptions
from smartystreets_python_sdk import StaticCredentials, exceptions, Batch, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup as StreetLookup
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

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

def read_and_validate_in_chunks(df_iterator: iter, config: dict, client) -> None:
    """
    This function reads the data frame iterator and iterates over them
    Each chuck is converted to a dict and sent to the smarty module
    """
    for index, df_chunk in enumerate(df_iterator):
        logger.info(f"processing chunk {index}")
        logger.info(f"chunk row count: {len(df_chunk.index)}")

        if (df_chunk.empty):
            logger.info("***************")
            logger.error("Empty dataframe detected. Exiting")
            logger.info("***************")
            exit(2)
        
        msg_batch = df_chunk.to_dict("records")
        # calculate the total message size 
        total_msg_size = len(json.dumps(msg_batch))
        logger.info(f"total_msg_size after adding the current chunk: {total_msg_size}")
        # send the message
        run_smarty_street_addr_lookup_batch(client, config, msg_batch, index)
        logger.info(f"Validation complete for chunk {index}")
        #logger.info(msg_batch)

# display api putput debug info
def print_debug_info(lookup: StreetLookup):
    candidates = lookup.result
    logger.info("Address {} has at least one candidate.".format(len(candidates)))
    logger.info("If the match parameter is set to STRICT, the address is valid.")
    logger.info("Otherwise, check the Analysis output fields to see if the address is valid.\n")
    for candidate in candidates:
        components = candidate.components
        metadata = candidate.metadata

        logger.info("\nCandidate {} : ".format(candidate.candidate_index))
        logger.info("Delivery line 1: {}".format(candidate.delivery_line_1))
        logger.info("Last line:       {}".format(candidate.last_line))
        logger.info("City:            {}".format(candidate.components.city_name))
        logger.info("State:           {}".format(candidate.components.state_abbreviation))
        logger.info("Zipcode:         {}".format(candidate.components.zipcode))
        logger.info("Plus 4:          {}".format(candidate.components.plus4_code))
        logger.info("County:          {}".format(candidate.components.county_fips))
        logger.info("DMA:             {}".format(candidate.components.dma_code))
        logger.info("Latitude:        {}".format(candidate.metadata.latitude))

# take smarty lookup object as input and write the input and output to s3 in parquet format
# this assumes US address format
def write_smarty_lookup_to_s3(batch: Batch, bucket: str, key: str):
    # write the input and output to s3 in json format
    logger.info("building pandas dataframe from the smarty lookup object")
    df = pd.DataFrame()
    invalid_addresses = 0
    for i, lookup in enumerate(batch):
        # logger.debug(vars(lookup))
        input = {
            "i_batch_index": i,
            "i_input_id": lookup.input_id,
            "i_full_addr_txt": lookup.street
        }
        candidates = lookup.result
        
        if len(candidates) == 0:
            logger.warning("Address {} is invalid.\n".format(i))
            # just append the valid flag
            input["o_valid"] = 0
            df_dictionary = pd.DataFrame([input])
            df = pd.concat([df,df_dictionary], ignore_index=True)
            invalid_addresses += 1
            continue
        # logger.debug(vars(candidates[0]))
        output = {
            "o_street_address1": f"{candidates[0].components.primary_number} {candidates[0].components.street_name} {candidates[0].components.street_suffix}",
            "o_street_address2": f"{candidates[0].components.secondary_number} {candidates[0].components.secondary_designator} {candidates[0].components.extra_secondary_designator} {candidates[0].components.extra_secondary_number}",
            "o_city": candidates[0].components.city_name,
            "o_state_code": candidates[0].components.state_abbreviation,
            "o_full_postal_code": f"{candidates[0].components.zipcode} {candidates[0].components.plus4_code}",
            "o_country": "USA",
            "o_external_addr_id": candidates[0].delivery_point_barcode,
            "o_valid": 1
        }

        inp_out = {**input, **output}
        # logger.info(inp_out)
        df_dictionary = pd.DataFrame([inp_out])
        df = pd.concat([df,df_dictionary], ignore_index=True)

    # logger.info(df.head())
    s3_uri = f"s3://{bucket}/{key}".replace(".","-")
    logger.info(f"Writing input and output to {s3_uri}")
    wr.s3.to_parquet(df, s3_uri)
    return invalid_addresses

# function to run smarty street address lookup
def run_smarty_street_addr_lookup_batch(client: ClientBuilder, config: dict, message_batch: list, list_index: int):
    
    # build the client and batch object

    batch = Batch()

    # Documentation for input fields can be found at:
    # https://smartystreets.com/docs/street-api#input-fields

    
    # add input street addresses
    # concat the components to create the fuill text as the validation services can split in to components
    # batch.add(StreetLookup("123 Main St", "San Francisco", "CA", "94105"))
    # [logger.info(message['address1'], message['city'], message['state_code'], message['zip_code']) for message in message_batch]
    # [batch.add(StreetLookup(street=message['address1'], city=message['city'], state=message['state_code'], zipcode=str(message['zip_code']),input_id=str(message['source_id']))) for message in message_batch]

    [batch.add(StreetLookup(street=utils.get_address_data_string(message, config["schema_map"]),input_id=str(message['source_id']))) for message in message_batch]

    # run the batch
    batch_size = len(batch)
    logger.info(batch_size)
    assert batch_size <= 100

    try:
        client.send_batch(batch)
    except exceptions.SmartyException as err:
        logger.error(err)
        return
    
    # print_debug_info(lookup)
    output_key = f"{config['s3_key']}/result-{list_index}.parquet"
    invalid_addresses = write_smarty_lookup_to_s3(batch, config['s3_bucket'], output_key)

    logger.info(f"Total/Invalid addresses in this batch:{batch_size}/{invalid_addresses}")

# main function that reads amazon step function input and calls the read_and_produce_df_chunk function
def main(config):
    # use named profile for local testing, in lamda env this will be(and has to be) None
    try:
        cli_profile = os.environ['CLI_PROFILE']
    except(KeyError):
        cli_profile = None
    bucket = config['bucket']
    key = config['key']
    chunk_size = int(config['chunk_size'])
    delimiter = config['delimiter']
    encoding = config['encoding']
    limit_rows = int(config['limit_rows'])
    ssm_parameter = config['ssm_parameter']

    smarty_config = utils.add_secrets_to_config(utils.get_app_configuration(ssm_parameter,cli_profile=cli_profile),cli_profile=cli_profile)

    # build smarty client
    credentials = StaticCredentials(smarty_config['secrets']['auth_id'], smarty_config['secrets']['auth_token'])
    client = ClientBuilder(credentials).with_licenses([smarty_config['license_key']]).build_us_street_api_client()
    
    df_iterator = read_s3_file_chunked(bucket, key, chunk_size, delimiter, encoding, limit_rows, cli_profile)

    read_and_validate_in_chunks(df_iterator, smarty_config, client)

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
                                'ssm_parameter'])
    # print(args)
    main(args)
    # main(get_sample_sfn_input_config())