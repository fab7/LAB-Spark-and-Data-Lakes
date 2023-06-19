#!/usr/bin/env python
# coding: utf-8

##############################################################################
#
# Answer Key to the Data Wrangling with DataFrames Coding Quiz
# 
# Helpful resources:
#   https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html
#
# Note:
#   When working with Spark DataFrames, there are two common methods for 
#   displaying the contents of a DataFrame: show() and print().
#   - The show() method is used to display a formatted view of the DataFrame
#     in a tabular format. It provides a concise and readable representation
#     of the data. By default, it displays the first 20 rows of the DataFrame.
#     You can specify the number of rows to display by passing an argument to
#     the show() method, like df.show(10) to display the first 10 rows.
#   - On the other hand, the print() method is used to display the contents of
#     the DataFrame in a more raw and unformatted way. It prints the entire
#     DataFrame to the console, which can be useful for debugging or inspecting
#     the data in a more detailed manner. However, it may not be as visually
#     appealing or easy to read as the show() method.
#   In summary, show() provides a formatted and concise view of the DataFrame,
#   while print() displays the raw contents of the DataFrame. The choice
#   between the two methods depends on your specific needs and preferences.
##############################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf, col
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import sys

# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

spark = SparkSession \
        .builder \
        .appName("Data Frames practice") \
        .getOrCreate()

logs_df = spark.read.json("../../data/sparkify_log_small.json")

logs_df.printSchema()

# Retrieve the value of argv[1]
question_num = "all"
if len(sys.argv) > 1: 
    question_num = sys.argv[1]
    print("The value of argv[1] is:", question_num)

if question_num == '1' or question_num == 'all':
    print()
    print("#######################################################################")
    print("# QUESTION 1 - Which page did user id "" (empty string) NOT visit?")
    print("#######################################################################")
    print("# Step-1a: Filter for users with blank user id")
    blank_user_df = logs_df.filter(logs_df.userId == '')
    blank_user_df.show(5)

    print("# Step-1b: Select column 'page', rename it to 'blank_pages' and drop duplicates")
    # NOTE: In Spark, the '.alias()' function is used to assign an alias or a new 
    #   name to a column in a DataFrame. In the current case, '.alias('blank_pages')'
    #   is used to assign the alias "blank_pages" to a column in the DataFrame. This
    #   means that the column will be referred to as "blank_pages" instead of its
    #   original name.
    #   Assigning an alias to a column can be useful for several reasons. It can
    #   make the code more readable and self-explanatory, especially when working
    #   with complex transformations or aggregations on DataFrames. It can also be
    #   helpful when joining or merging DataFrames, as it allows you to refer to
    #   columns by their aliases instead of their original names, which can make
    #   the code more concise and easier to understand.
    blank_pages_df =  logs_df.filter(logs_df.userId == '') \
                        .select(col('page')) \
                        .alias('blank_pages') \
                        .dropDuplicates()
    blank_pages_df.show(5)

    print("# Step-2: Get a list of possible pages that could be visited")
    all_pages_df = logs_df.select('page').dropDuplicates()
    all_pages_df.show()

    print("# Step-3: Find values in all_pages that are not in blank_pages")
    print("#   These are the pages that the blank user did not go to")
    # NOTE-1: WE SHOULD NOT USE .collect() on large datasets (>100 MB)
    # NOTE-2: set(all_pages_df.collect()) and set(blank_pages_df.collect()) convert
    #   the lists of Row objects into sets. Sets are unordered collections of unique
    #   elements.
    # NOTE-3: set(all_pages_df.collect()) - set(blank_pages_df.collect()) calculates
    #   the set difference between the two sets. It returns a new set that contains
    #   only the elements that are present in the first set (all_pages_df.collect())
    #   but not in the second set (blank_pages_df.collect()                                                                                                                                                                                                      
    for row in set(all_pages_df.collect()) - set(blank_pages_df.collect()):
        print(row.page)


if question_num == '2' or question_num == 'all':
    print()
    print("#######################################################################")
    print("# QUESTION 2 - What type of user does the empty string user id most likely refer to?")
    print("#######################################################################")
    print("# Perhaps it represents users who have not signed up yet or who are signed out and are about to log in.")

if question_num == '3' or question_num == 'all':
    print()
    print("#######################################################################")
    print("# QUESTION 3 - How many female users do we have in the data set?")
    print("#######################################################################")
    print("# Step-3a: Filter for users with gender == 'F' ")
    female_users_df = logs_df.filter(logs_df.gender == 'F')
    female_users_df.show(5)

    print("# Step-3b: Select column 'userId' and 'gender', drop duplicates and count") 
    print(
    logs_df.filter(logs_df.gender == 'F')  \
        .select('userId', 'gender') \
        .dropDuplicates() \
        .count()
    )

if question_num == '4' or question_num == 'all':
    print()
    print("#######################################################################")
    print("# QUESTION 4 - How many songs were played from the most played artist?")
    print("#######################################################################")
    artists_df = logs_df.filter(logs_df.page == 'NextSong') \
        .select('Artist') \
        .groupBy('Artist') \
        .agg({'Artist':'count'}) \
        .withColumnRenamed('count(Artist)', 'Playcount') \
        .sort(desc('Playcount')) \
        .show(1)   
        
if question_num == '5' or question_num == 'all':
    print()
    print("#######################################################################")
    print("# QUESTION 5 - How many songs do users listen to on average between visiting our home page?")
    print("#######################################################################")
    # Please round your answer to the closest integer.
    
    # TODO: filter out 0 sum and max sum to get more exact answer

    print("# Step-5a: Create a window, partition it by 'userID' and order it by 'ts' ")
    user_window = Window \
        .partitionBy('userID') \
        .orderBy(desc('ts')) \
        .rangeBetween(Window.unboundedPreceding, 0)

    ishome = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

    print("# Step-5b: Filter only NextSong and Home pages, add 1 for each time they visit Home")
    # Adding a column called period which is a specific interval between Home visits
    cusum = logs_df.filter((logs_df.page == 'NextSong') | (logs_df.page == 'Home')) \
        .select('userID', 'page', 'ts') \
        .withColumn('homevisit', ishome(col('page'))) \
        .withColumn('period', Fsum('homevisit') \
        .over(user_window)) 
        
    # This will only show 'Home' in the first several rows due to default sorting
    cusum.show(300)


    # See how many songs were listened to on average during each period
    cusum.filter((cusum.page == 'NextSong')) \
        .groupBy('userID', 'period') \
        .agg({'period':'count'}) \
        .agg({'count(period)':'avg'}) \
        .show()



