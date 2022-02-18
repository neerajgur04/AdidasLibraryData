# Databricks notebook source
#uploaded the login credential IAM user file required to mount the AWS S3 bucket on Databricks  
display(dbutils.fs.ls("/FileStore/tables"))

# COMMAND ----------

#import library required to handle dataframes and URLs
from pyspark.sql.functions import *
import urllib

# COMMAND ----------

file_type = "csv"
first_row_is_header = "true"
delimiter = ","

aws_keys_df = spark.read.format(file_type)\
.option("header",first_row_is_header)\
.option("sep",delimiter)\
.load("/FileStore/tables/new_user_credentials.csv")


# COMMAND ----------

#Define Access_Key and Secret_key referencing the user credential file uploaded above
ACCESS_KEY = aws_keys_df.where(col('User name') == 'DBS3user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User Name') == 'DBS3user').select('Secret access key').collect()[0]['Secret access key']
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe = "")


# COMMAND ----------

# Mount S3 buckets on Databricks file system
AWS_S3_BUCKET = "librarydata04"
MOUNT_NAME = "/mnt/librarydata"
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY,ENCODED_SECRET_KEY,AWS_S3_BUCKET)
dbutils.fs.mount(SOURCE_URL,MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/librarydata/stg"))

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget --continue https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O /dbfs/mnt/librarydata/stg/ol_cdump.json

# COMMAND ----------

# MAGIC %fs ls /mnt/librarydata/stg

# COMMAND ----------


