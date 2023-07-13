import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Added parameters 
args = getResolvedOptions(sys.argv, ["JOB_NAME","sourcebucket","targetbucket","sourcetable","targettable","targetcatalogdb", "targetcatalogtable"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# set parameters
sourcebucket=str(args["sourcebucket"])
targetbucket=str(args["targetbucket"])
sourcetable=str(args["sourcetable"])
targettable=str(args["targettable"])
targetcatalogdb=str(args["targetcatalogdb"])
targetcatalogtable=str(args["targetcatalogtable"])

# Script generated for node S3 bucket
normalize_output_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        # cleanroom output comes with header and delimiter is comma for CSV
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            f"s3://{sourcebucket}/{sourcetable}/"
        ]
    },
    transformation_ctx="normalize_output_node1",
)

# Script generated for node ApplyMapping
# Since CSV source is used all columns are read as string first 
# and data type conversion is needed
ApplyMapping_node2 = ApplyMapping.apply(
    frame=normalize_output_node1,
    mappings=[
        ("c_customer_id", "string", "c_customer_id", "string"),
        ("c_first_name", "string", "c_first_name", "string"),
        ("c_last_name", "string", "c_last_name", "string"),
        ("c_birth_day", "string", "c_birth_day", "int"),
        ("c_birth_month", "string", "c_birth_month", "int"),
        ("c_birth_year", "string", "c_birth_year", "int"),
        ("c_email_address", "string", "c_email_address", "string"),
        ("ss_net_paid", "string", "ss_net_paid", "float"),
    ],
    transformation_ctx="ApplyMapping_node2",
)
# Script generated for node normalize
SqlQuery0 = """
select 
    trim(c_customer_id) as c_customer_id,
    trim(lower(c_first_name)) as c_first_name,
    trim(lower(c_last_name)) as c_last_name,
    c_birth_day,
    c_birth_month, 
    c_birth_year,
    trim(lower(c_email_address)) as c_email_address,
    sum(ss_net_paid) as ss_net_paid
from myDataSource
group by
    trim(c_customer_id),
    trim(lower(c_first_name)),
    trim(lower(c_last_name)),
    c_birth_day,
    c_birth_month, 
    c_birth_year,
    trim(lower(c_email_address))
having sum(ss_net_paid) > 5000
"""
normalize_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": ApplyMapping_node2},
    transformation_ctx="normalize_node3",
)

# Script generated for node S3 bucket
TargetS3bucket_node4 = glueContext.getSink(
    path=f"s3://{targetbucket}/{targettable}/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TargetS3bucket_node4",
)
TargetS3bucket_node4.setCatalogInfo(
    catalogDatabase=targetcatalogdb,
    catalogTableName=targetcatalogtable,
)
TargetS3bucket_node4.setFormat("csv")
TargetS3bucket_node4.writeFrame(normalize_node3)
job.commit()
