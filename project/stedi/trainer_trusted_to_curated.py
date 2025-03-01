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

# Script generated for node CustomerCurated
CustomerCurated_node1687279486586 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1687279486586",
)

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1687424545672 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://fab-se4s-bucket/stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1687424545672",
)

# Script generated for node Join-On-SerialNumbers
JoinOnSerialNumbers_node1687424380256 = Join.apply(
    frame1=CustomerCurated_node1687279486586,
    frame2=StepTrainerLanding_node1687424545672,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinOnSerialNumbers_node1687424380256",
)

# Script generated for node Drop-CustomerFields
DropCustomerFields_node1687424791995 = DropFields.apply(
    frame=JoinOnSerialNumbers_node1687424380256,
    paths=[
        "customername",
        "email",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "sharewithresearchasofdate",
    ],
    transformation_ctx="DropCustomerFields_node1687424791995",
)

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1687280279836 = glueContext.write_dynamic_frame.from_options(
    frame=DropCustomerFields_node1687424791995,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://fab-se4s-bucket/stedi/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1687280279836",
)

job.commit()
