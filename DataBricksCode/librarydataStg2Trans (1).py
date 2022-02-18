# Databricks notebook source
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# Load data into a dataframe for futher cleaning process 
dflibdata= spark.read.load("/mnt/librarydata/stg/ol_cdump.json", format = 'json')

# COMMAND ----------

# Total number of records in raw data set provided
dflibdata.count()

# COMMAND ----------

# Get familiar with schema
# Can see we have few struct fields in the dataset
dflibdata.printSchema()

# COMMAND ----------

# Have a look at the raw dataset
display(dflibdata)

# COMMAND ----------

# Data Cleaning#1
# Extracting a subset of dataset with only fields required for our analysis
dflibdata1_1= dflibdata.select(dflibdata.authors, dflibdata.genres, dflibdata.number_of_pages, dflibdata.publish_date, dflibdata.publishers , dflibdata.title,dflibdata.created.value)
dflibdata1 = dflibdata1_1.withColumnRenamed("created.value","created")

# COMMAND ----------

# Data Cleaning#2
# Drop duplicate records if any
dflibdata2 = dflibdata1.dropDuplicates()

# COMMAND ----------

# Data Cleaning#3: 
# Drop records with NULL values
dflibdata3 = dflibdata2.filter(dflibdata2.authors.isNotNull()).filter(dflibdata2.number_of_pages.isNotNull())\
    .filter(dflibdata2.publish_date.isNotNull()).filter(dflibdata2.title.isNotNull()).filter(dflibdata2.created.isNotNull())

# COMMAND ----------

# Data Cleaning#4: 
# Genres is an array field so cleaning and fetching the genres in a new column out of this array element
# so exploding this array element to have multiple rows for each of the array element under genres field
dflibdata4 = dflibdata3.filter(dflibdata3.genres.isNotNull())
dflibdata5 = dflibdata4.select(dflibdata4.authors, dflibdata4.genres,dflibdata4.number_of_pages,dflibdata4.publish_date, \
                               dflibdata4.publishers , dflibdata4.title,dflibdata4.created)

# COMMAND ----------

# Data Cleaning#5: Publish_date:
# this date is of type STRING and having data in various format as mentioned below:
# 23 Sept 2010, 2012 etc and also have nulls
# Fetch the year from this field into a new field having year information.
# ignored all the null values and
# all those records where publish_year is before 1950
dflibdata6 = dflibdata5.withColumn("publish_year_new",substring(dflibdata5.publish_date,-4,4).cast('Int'))
dflibdata7 = dflibdata6.filter(dflibdata6.publish_year_new > 1950)
# validate that now the dataset do not have any record with publish_year less than 1950.
dflibdata7.select(min(dflibdata7.publish_year_new)).show()

# COMMAND ----------

# Data Cleaning#6: 
# number_of_pages
# we have records present having number_of_pages less than 20 and also few records with null values
# so filter out such records
dflibdata8 = dflibdata7.filter(dflibdata7.number_of_pages > 20)
dflibdata9 = dflibdata8.filter(dflibdata8.number_of_pages.isNotNull())
# Validate that after this data cleaning, we do not have any record with number_of_pages less than 20 or with NULL
dflibdata9.select (min(dflibdata9.number_of_pages)).show()
dflibdata9.where (dflibdata9.number_of_pages.isNull()).agg(count(col("*")).alias("number of records with NULL")).show()

# COMMAND ----------

# Data Cleaning#7: 
# Title field
# we have records having title as NULL and also few records with title as blank
# filter out such records
dflibdata10 = dflibdata9.filter(dflibdata9.title.isNotNull())
dflibdata11 = dflibdata10.filter(dflibdata10.title != "")
# Validat that after the data cleaning, we do not have any record in dataset with title as NULL or blank
dflibdata11.where (dflibdata11.title.isNull()).agg(count(col("*")).alias("number of records with NULL")).show()
dflibdata11.where (dflibdata11.title == "").agg(count(col("*")).alias("number of records with blank title")).show()

# COMMAND ----------

# Derive new columns month, year from created column, which are required for our business requirement#5
yearrange = (1950, 2021)
dflibdata12 = dflibdata11.withColumn("createtime",to_timestamp(col("created")))
dflibdata13 = dflibdata12.withColumn("createdate",to_date(col("created")))
dflibdata14 = dflibdata13.withColumn("createmonth",month(col("createdate")))
dflibdata15 = dflibdata14.withColumn("createyear",year(col("createdate")))
dflibdata16 = dflibdata15.filter(col("createyear").between(*yearrange))

# COMMAND ----------

#Saving the cleansed data into Delta Lake
dflibdata16.coalesce(1).write.format("delta").mode("overwrite").save("/mnt/librarydata/trans")

# COMMAND ----------

# Check and make sure data is saved properly in trans folder
display(dbutils.fs.ls("/mnt/librarydata/trans"))
# can see below that the cleaned data saved in the Delta format.
