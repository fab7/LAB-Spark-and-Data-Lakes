import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1687542826746 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1687542826746",
)

# Script generated for node CustomerLanding
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://fab-se4s-bucket/stedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node ShareWithResearch
ShareWithResearch_node1687367716280 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node1687367716280",
)

# Script generated for node Join-On-Email
JoinOnEmail_node1687542861275 = Join.apply(
    frame1=ShareWithResearch_node1687367716280,
    frame2=AccelerometerLanding_node1687542826746,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinOnEmail_node1687542861275",
)

# Script generated for node Drop Fields
DropFields_node1687542918060 = DropFields.apply(
    frame=JoinOnEmail_node1687542861275,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1687542918060",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687542918060,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://fab-se4s-bucket/stedi/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node3",
)

job.commit()
