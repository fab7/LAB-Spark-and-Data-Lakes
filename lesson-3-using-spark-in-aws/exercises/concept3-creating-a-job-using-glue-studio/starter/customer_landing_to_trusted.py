import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://fab-se4s-bucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="PrivacyFilter_node2",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://fab-se4s-bucket/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node3",
)

job.commit()
