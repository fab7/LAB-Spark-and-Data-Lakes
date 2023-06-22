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

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1687279493900 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_Landing_node1687279493900",
)

# Script generated for node CustomerLanding
CustomerLanding_node1687279486586 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1687279486586",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1687279504268 = Join.apply(
    frame1=CustomerLanding_node1687279486586,
    frame2=Accelerometer_Landing_node1687279493900,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyJoin_node1687279504268",
)

# Script generated for node Drop Fields
DropFields_node1687279768329 = DropFields.apply(
    frame=CustomerPrivacyJoin_node1687279504268,
    paths=[],
    transformation_ctx="DropFields_node1687279768329",
)

# Script generated for node CustomerCurated
CustomerCurated_node1687280279836 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687279768329,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://fab-se4s-bucket/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1687280279836",
)

job.commit()
