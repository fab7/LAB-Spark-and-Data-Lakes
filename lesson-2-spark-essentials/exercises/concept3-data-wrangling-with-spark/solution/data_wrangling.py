##############################################################################
# 
#  DATA WRANGLING with SPARK
# 
#    Run each code cell to understand what the code does and how it works.
#
##############################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#==========================================================
# Instantiate a SparkSession and then read in the data set
#==========================================================
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

path = "../../data/sparkify_log_small.json"
user_log_df = spark.read.json(path)

print("\n#==========================================================")
print("# Data Exploration - Let's explore the data set.           ")
print("#==========================================================")

print("\n# take(5) - These are the 5 first records") 
print(
    user_log_df.take(5)
)

print("\n# printSchema() - This the schema inferred by Spark")
user_log_df.printSchema()

print("\n# show() - Describe the dataframe")
user_log_df.describe().show()

print("\n# describe().show)() - Describe the statistics for the song length column")
user_log_df.describe("length").show()

print("\n# count() - Count the rows in the dataframe")
print(
    user_log_df.count()
)    

print("\n# select().dropDuplicates().sort().show() - Select the page column, drop the duplicates, and sort by page")
user_log_df.select("page").dropDuplicates().sort("page").show()

print("\n# select().where().show() - Select data for all pages where userId is 1046")
user_log_df.select(["userId", "firstname", "page", "song"]) \
    .where(user_log_df.userId == "1046") \
    .show()

print("\n# Calculating Statistics by Hour")
# NOTE: 'udf()' creates a user defined function (UDF).
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)

# Now that we have created that UDF, we can use it as a new column
# that we name 'hour'.
user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))

print("\n# head() - Let's get the first row to see if the new column was added")
print(
    user_log_df.head(1)
)

print("\n# filter().groupby().count().orderBy() - Select just the NextSong page")
songs_in_hour_df = user_log_df.filter(user_log_df.page == "NextSong") \
    .groupby(user_log_df.hour) \
    .count() \
    .orderBy(user_log_df.hour.cast("float"))

songs_in_hour_df.show()

print("\n# toPAndas() - For scatter plot")
songs_in_hour_pd = songs_in_hour_df.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played")
plt.show()

print("\n# dropna() - Drop Rows with Missing Values")
# As you'll see, it turns out there are no missing values in the userID or 
# session columns. But there are userID values that are empty strings.
# how = 'any' or 'all'. 
#  - If 'any', drop a row if it contains any nulls. 
#  - If 'all', drop a row only if all its values are null.
# subset = list of columns to consider
user_log_valid_df = user_log_df.dropna(how = "any", subset = ["userId", "sessionId"])

print("\n # How many are there now that we dropped rows with null userId or sessionId?")
print(
    user_log_valid_df.count()
)

print("\n# select().dropDuplicates().sort() - Select all unique user IDs into a dataframe")
user_log_df.select("userId") \
    .dropDuplicates() \
    .sort("userId").show()

print("\n# filter() - Select only data for where the userId column isn't an empty string (different from null)")
user_log_valid_df = user_log_valid_df.filter(user_log_valid_df["userId"] != "")

print("\n# Notice the count has dropped after dropping rows with empty userId")
print(
    user_log_valid_df.count()
)

print("\n#==========================================================")
print("# Find users who downgraded their accounts                 ")
print("#==========================================================")

print("\n# Users who downgraded their accounts")
user_log_valid_df.filter("page = 'Submit Downgrade'") \
    .show()

user_log_df.select(["userId", "firstname", "page", "level", "song"]) \
    .where(user_log_df.userId == "1138") \
    .show()

# Create a user defined function to return a 1 if the record contains a downgrade
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

print("\n# Add a column that flags the downgraded accounts")
user_log_valid_df = user_log_valid_df \
    .withColumn("downgraded", flag_downgrade_event("page"))

print(
    user_log_valid_df.head()
)

from pyspark.sql import Window

print("\n# partitionBy().orderBy().rangeBetween() - Partition by user id")
# This uses a window function and cumulative sum to distinguish each user's 
# data as either pre or post downgrade events.
windowval = Window.partitionBy("userId") \
    .orderBy(desc("ts")) \
    .rangeBetween(Window.unboundedPreceding, 0)

print("\n# Fsum() - Cumulative sum over a window showing all events for a user")
# Add a column called 'phase', with '0' if the user hasn't downgraded yet, 
# or '1' if they have
user_log_valid_df = user_log_valid_df \
    .withColumn("phase", Fsum("downgraded") \
    .over(windowval))

user_log_valid_df.show()    

print("\n# Show the phases for user 1138") 
user_log_valid_df \
    .select(["userId", "firstname", "ts", "page", "level", "phase"]) \
    .where(user_log_df.userId == "1138") \
    .sort("ts") \
    .show()

