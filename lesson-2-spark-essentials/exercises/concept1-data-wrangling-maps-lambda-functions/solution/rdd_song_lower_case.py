### 
# You might have noticed this code in the screencast.
#
# import findspark
# findspark.init('spark-2.3.2-bin-hadoop2.7')
#
# The findspark Python module makes it easier to install
# Spark in local mode on your computer. This is convenient
# for practicing Spark syntax locally. 
# However, the workspaces already have Spark installed and you do not
# need to use the findspark module
#
###


#-----------------------------------------------------------------------------
#-- PySpark and SparkSession
#-----------------------------------------------------------------------------
# Python is one of many languages you can use to write Spark Jobs.
# If you choose to use Python, then you will use the PySpark library.
# PySpark gives you access to all the important Spark data constructs like:
#   - RDDs, DataFrames and Spark SQL.
# That means you can write Spark code that runs in either a Spark
# Cluster, in a Jupyter Notebook, or on your laptop. When you write code on
# your Jupyter Notebook or a laptop, Spark creates a temporary Spark node
# that runs locally. Because Spark uses Java, it is necessary to install the
# JDK on a computer used to run PySpark code.
#-----------------------------------------------------------------------------
from pyspark.sql import SparkSession

#-----------------------------------------------------------------------------
#-- The Spark Context
#-----------------------------------------------------------------------------
# The first component of each Spark Program is the `SparkContext`. 
# The `SparkContext` is the main entry point for Spark functionality and 
# connects the cluster with the application.
# To create a `SparkContext`, we first need a `SparkConf` object to specify
# some information about the application such as its name and the master's
# nodes' IP address. Note that if we run Spark in local mode, we can just put
# the string `local` as master.
#
# NOTE: The `SparkContext` is typically used when you want to use low-level 
#   abstractions. Otherwise, to read data frames, we need to use a Spark SQL
#   equivalent, the `SparkSession`.
#-----------------------------------------------------------------------------
# from pyspark import SparkContext, SparkConf
# configure = SparkConf().setAppNAme("name").setMaster("IP Address")
# sc = pyspark.SparkContext(conf = configure)


#-----------------------------------------------------------------------------
#-- The Spark Session
#-----------------------------------------------------------------------------
# To read higher-level abstractions such as data frames, we must use a Spark
# SQL equivalent, the `SparkSession`. 
# Similarity to the `SparkConf`, we can specify some parameters to create a
# `SparkSession`.
#   - `getOrCreate()` for example, means that if you already have a 
#     SparkSession running, instead of creating a new one, the old one will be
#     returned and its parameters will be modified to the new configurations.
#
# In the line below, because we aren't running on a spark cluster, the session
# is just for development.
#-----------------------------------------------------------------------------
ss = SparkSession \
    .builder \
    .appName("Maps and Lazy Evaluation Example") \
    .getOrCreate()

# Starting off with a regular python list
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]

# Convert the log of songs to a distributed dataset that Spark can use.
# This uses a special `spark.sparkContext` object which comes with a method to  
# parallelize a Python object and distribute it across the machines in your
# cluster, so that Spark can process the dataset.
# NOTE: The `distributed_song_log_rdd` is an RDD (Resilient Distributed Dataset)
#-----------------------------------------------------------------------------
distributed_song_log_rdd = ss.sparkContext.parallelize(log_of_songs)

# Show that the original input data is preserved
# Notice that we DO NOT use the `.collect()` method but the `.foreach()` one.
# The difference between teh two methods is: 
#   `.collect()` forces all the data from the entire RDD on all nodes to be
#    collected from ALL the nodes, which kills productivity, and could crash.
#   `.foreach()` allows the data to stay on each of the independent nodes.
#-----------------------------------------------------------------------------
print('\nTHIS IS THE ORIGINAL INPUT DATASET')
distributed_song_log_rdd.foreach(print)

def convert_song_to_lowercase(song):
    return song.lower()

# Next, we can use the Spark function `map` to apply the lowercase conversion
# function to each song in our dataset.
# NOTE: The method `toDF()` can be used to convert from an RDD to a DataFrame.
#   This allows us to use convenient functions like `show()`. 
lower_case_songs=distributed_song_log_rdd.map(convert_song_to_lowercase)

print('\nTHIS IS THE PROCESSED DATASET')
lower_case_songs.foreach(print)

# Show the original input data is still mixed case
print('\nBUT THE ORIGINAL INPUT DATASET REMAINS UNCHANGED')
distributed_song_log_rdd.foreach(print)

# Finally, use lambda functions instead of named functions to do the same 
# map operation
print('\nTHIS IS THE SAME DATASET PROCESSED BY: (lambda song: song.lower())')
distributed_song_log_rdd.map(lambda song: song.lower()).foreach(print)
print('\nTHIS IS THE SAME DATASET PROCESSED BY: (lambda    x:    x.lower())')
distributed_song_log_rdd.map(lambda x: x.lower()).foreach(print)