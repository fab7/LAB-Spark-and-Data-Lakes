##############################################################################
#
# Reading and Writing into Spark Data Frames
#
# In this example, we show how we can use Spark to wrangle and explore data
# using the DataFrame API. In this exercise, we'll import and export data to
# and from Spark DataFrames using a dataset that describes log events coming
# from a music streaming service.
#
# NOTE: Instead of reading in a dataset from a remote cluster, the data set
#   is read in from a local file.
#
##############################################################################
from pyspark.sql import SparkSession


# Since we're using Spark locally, we already have both a 'SparkContext' and a
# 'SparkSession' running. We can update some of the parameters, such our 
# application's name (let's just call it "Our first Python Spark SQL example").
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

# Let's check if the change went through by printing the default configuration
print("\n=== THIS IS THE DEFAULT CONFIGURATION ============")
print(
    spark.sparkContext.getConf().getAll()
)    

# Let's create our first dataframe from a sample data stored in a local JSON
# file (not HDFS yet).
# NOTE: Througout the lesson we'll work with a log file data set that describes
#   user interactions with a music streaming service. The records describe 
#   events such as logging in to the site, visiting a page, listening to the
#   next song, seeing an ad.
path = "../../data/sparkify_log_small.json"
user_log_df = spark.read.json(path)

# Let's print the schema to see how Spark inferred it from the JSON file
print("\n=== THIS IS THE SCHEMA INFERRED BY SPARK =========")
user_log_df.printSchema()

print("=== AND THIS IS WHAT WE CAN LEARN FROM DESCRIBE ====")
print(
    user_log_df.describe()
)

print("\n=== THIS IS THE CONTENT OF THE FIRST ROW =========")
user_log_df.show(n=1)


print("===\n THIS IS THE CONTENT OF THE 5 TOP ROWS ========")
print(
    user_log_df.take(5)
)


#
# NOW, LET US CHANGE THE FILE FORMAT FROM JSON TO CSV
# 
out_path = "../../data/sparkify_log_small.csv"


# WARNING: The filename alone didn't tell Spark the actual format.
#   We need to do it explictely like this.
user_log_df.write.mode("overwrite").save(out_path, format="csv", header=True)

# Notice we have created another dataframe here
# We wouldn't usually read the data again. However, this does show that
# the read method works the same with different data types.
user_log_2_df = spark.read.csv(out_path, header=True)
print("\n=== THIS IS THE SCHEMA INFERRED FROM CSV =========")
user_log_2_df.printSchema()

# Choose two records from the CSV file
print("===\n THIS IS THE CONTENT OF THE 2 TOP ROWS ========")
print(
    user_log_2_df.take(2)
)    

# Show the userID column for the first several rows
user_log_2_df.select("userID").show()

# 
print(
user_log_2_df.take(1)
)