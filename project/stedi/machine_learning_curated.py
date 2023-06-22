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

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1687279486586 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1687279486586",
)

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1687424545672 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1687424545672",
)

# Script generated for node Join-On-TimeStamp
JoinOnTimeStamp_node1687424380256 = Join.apply(
    frame1=AccelerometerTrusted_node1687279486586,
    frame2=StepTrainerTrusted_node1687424545672,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="JoinOnTimeStamp_node1687424380256",
)

# Script generated for node Drop-RedundantFields
DropRedundantFields_node1687424791995 = DropFields.apply(
    frame=JoinOnTimeStamp_node1687424380256,
    paths=["sensorreadingtime"],
    transformation_ctx="DropRedundantFields_node1687424791995",
)

# Script generated for node MachineLearningCurated
MachineLearningCurated_node1687280279836 = glueContext.write_dynamic_frame.from_options(
    frame=DropRedundantFields_node1687424791995,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://fab-se4s-bucket/stedi/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1687280279836",
)

job.commit()
