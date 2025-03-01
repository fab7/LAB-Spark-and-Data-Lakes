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

# Script generated for node CustomerTrusted
CustomerTrusted_node1687279486586 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1687279486586",
)

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1687424545672 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1687424545672",
)

# Script generated for node Join-On-Email-And-User
JoinOnEmailAndUser_node1687424380256 = Join.apply(
    frame1=CustomerTrusted_node1687279486586,
    frame2=AccelerometerTrusted_node1687424545672,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinOnEmailAndUser_node1687424380256",
)

# Script generated for node Drop-SensorRecords
DropSensorRecords_node1687424791995 = DropFields.apply(
    frame=JoinOnEmailAndUser_node1687424380256,
    paths=[],
    transformation_ctx="DropSensorRecords_node1687424791995",
)

# Script generated for node CustomerCurated
CustomerCurated_node1687280279836 = glueContext.write_dynamic_frame.from_options(
    frame=DropSensorRecords_node1687424791995,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://fab-se4s-bucket/stedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1687280279836",
)

job.commit()
