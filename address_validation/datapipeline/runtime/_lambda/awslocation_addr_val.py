import json
import boto3
from util import utils
import os
import logging
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# display api putput debug info
def print_debug_info(lookup):
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

# take the validation response dict list as input and write to s3 in parquet format
def write_val_output_to_s3(batch: list, bucket: str, key: str, message_id: str):
    # write the input and output to s3 in json format
    logger.info("building pandas dataframe from the aws location services response")
    df = pd.DataFrame()
    invalid_addresses = 0

    for i, lookup in enumerate(batch):
        # logger.info(lookup)
        # we are taking only the selected fields from the input message
        # modify this as required

        input = {
            "i_input_msg_id": message_id,
            "i_batch_index": i,
            "i_input_id": lookup["source_id"],
            "i_full_addr_txt": lookup["full_addr_txt"]
        }
        
        candidates = lookup["Results"]

        # with aws loc services if address is accurately identified relevance will be 1
        # Here we are replicating what a STRICT validation looks like with smarty api
        if candidates[0]['Relevance'] < 1:
            logger.warning("Address {} is invalid.\n".format(i))
            output = { "o_valid": 0 }
            invalid_addresses += 1
        # logger.debug(vars(candidates[0]))
        # the output schema changes with the validation api
        # below assumes an US address schema and the current venice attribute schema
        else:
            try:
                o_street_address2 = f'{candidates[0]["Place"]["UnitType"]} {candidates[0]["Place"]["UnitNumber"]}'
            except KeyError:
                o_street_address2 = None

            try:
                o_external_addr_id = candidates[0]["PlaceId"]
            except KeyError:
                o_external_addr_id = None

            output = {
                "o_street_address1": f'{candidates[0]["Place"]["AddressNumber"]} {candidates[0]["Place"]["Street"]}',
                "o_street_address2": o_street_address2,
                "o_city": candidates[0]["Place"]["Municipality"],
                "o_state": candidates[0]["Place"]["Region"],
                "o_full_postal_code": candidates[0]["Place"]["PostalCode"],
                "o_country": candidates[0]["Place"]["Country"],
                "o_external_addr_id": o_external_addr_id,
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


# function to run aws location services address lookup
def run_awslocation_addr_lookup(config: dict, message_batch: list, msg_id: str, cli_profile: str=None):
    
    # build the client
    session = boto3.session.Session(profile_name=cli_profile)
    awsloc_client = session.client('location')
    place_index = config['place_index']
    # logger.info(awsloc_client.list_place_indexes())
    batch=[]
    # add input street addresses
    # concat the components to create the fuill text as the validation services can split in to components
    # batch.add(StreetLookup("123 Main St", "San Francisco", "CA", "94105"))
    # [logger.info(message['address1'], message['city'], message['state_code'], message['zip_code']) for message in message_batch]
    for message in message_batch:
        # logger.info(message)
        # addr_txt = f"{message['address1']} {message['city']} {message['state_code']} {message['zip_code']}"
        message["full_addr_txt"] = utils.get_address_data_string(message, config["schema_map"])
        # logger.info(addr_txt)
        try:
            response = awsloc_client.search_place_index_for_text(
                IndexName=place_index,
                MaxResults=1,
                Text=message["full_addr_txt"]
            )
        except Exception as e:
            logger.error(e)
            return
        inp_out = {**message, **response}
        batch.append(inp_out)
        # logger.info(json.dumps(inp_out, indent=4, sort_keys=True))
    # print_debug_info(lookup)
    output_key = f"{config['s3_key']}-{msg_id}-result"
    batch_size = len(batch)
    logger.info(f"Total addresses in this batch:{batch_size}")

    invalid_addresses = write_val_output_to_s3(batch, config['s3_bucket'], output_key, msg_id)

    logger.info(f"Total/Invalid addresses in this batch:{batch_size}/{invalid_addresses}")


# lambda handler that reads amazon step function input and calls the read_and_produce_df_chunk function
def lambda_handler(event, context):
    """
    Lambda handler function
    """
    # load configurations
    ssm_parameter = os.environ['SSM_PARAMETER']
    # use named profile for local testing, in lamda env this will be(and has to be) None
    try:
        cli_profile = os.environ['CLI_PROFILE']
    except(KeyError):
        cli_profile = None

    config = utils.get_app_configuration(ssm_parameter,cli_profile=cli_profile)

    # logger.info(config_json)

    # iterate over event['records'] extract data
    
    msg_id = event['Records'][0]['messageId']
    for record in event['Records']:
        # logger.info(record['body'])
        message_batch = json.loads(record['body'])
        run_awslocation_addr_lookup(config, message_batch, msg_id, cli_profile)

    # for custom api calls if validation service does not have an python SDK
    # config_json = utils.get_sample_configuration()
    # url = utils.build_address_validation_smarty_url(config_json)
    # logger.info(url)
    # data = utils.generate_sample_physical_address()
    # logger.info(data)
    # utils.make_http_post_request(url, data)
    
# # use this for local testing through cli
# if __name__ == "__main__":
#     lambda_handler(utils.get_sample_sqs_message_input(), None)
