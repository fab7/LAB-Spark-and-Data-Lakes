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

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1687279493900 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1687279493900",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node1687279486586 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1687279486586",
)

# Script generated for node CustomerPrivacyJoin
CustomerPrivacyJoin_node1687279504268 = Join.apply(
    frame1=CustomerTrusted_node1687279486586,
    frame2=AccelerometerLanding_node1687279493900,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyJoin_node1687279504268",
)

# Script generated for node DropFields
DropFields_node1687279768329 = DropFields.apply(
    frame=CustomerPrivacyJoin_node1687279504268,
    paths=[
        "phone",
        "customername",
        "email",
        "birthday",
        "serialnumber",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "lastupdatedate",
        "registrationdate",
    ],
    transformation_ctx="DropFields_node1687279768329",
)

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1687280279836 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687279768329,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://fab-se4s-bucket/stedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1687280279836",
)

job.commit()
