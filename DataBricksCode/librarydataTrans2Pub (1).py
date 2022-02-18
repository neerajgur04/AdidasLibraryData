# Databricks notebook source
# Reading the data from trans layer (cleaned data)
# We will be running the business rules and derive any metrix in this step and will get the data/ insight required by business to see on the dashboard

librarydatacleaned = spark.read.format("delta").load("/mnt/librarydata/trans/")

# COMMAND ----------

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

dflibdatagenres = librarydatacleaned.select(explode(librarydatacleaned.genres).alias("newgenres"),librarydatacleaned.title)

# COMMAND ----------

dflibdatagenres.createOrReplaceTempView("librarydatagenres")

# COMMAND ----------

display(dflibdatagenres)

# COMMAND ----------

display(librarydatacleaned)

# COMMAND ----------

librarydatacleaned.createOrReplaceTempView("librarydata")

# COMMAND ----------

spark.sql("select count(*) from librarydata").show()

# COMMAND ----------

# Task#1: Get the book with the most pages
spark.sql("select title as Book,number_of_pages as NumberofPages from librarydata where number_of_pages = (select max(number_of_pages) from librarydata )").show(truncate=False)


# COMMAND ----------

# Task#2: Find the top 5 genres with most books
spark.sql("select newgenres as Genres, titlecount as BookCount from (select newgenres, count(title) as titlecount from librarydatagenres group by newgenres) a order by titlecount desc limit 5").show( truncate=False)


# COMMAND ----------

# Task#3: Retrieve the top 5 authors who (co-)authored the most books
spark.sql("select authors,title from librarydata where title in (SELECT title FROM librarydata GROUP BY title having count(authors) >1)").show(5,truncate=False)

# COMMAND ----------

# Task#4: Per publish year, get the number of authors that published at least one book.
spark.sql("select publish_year_new as PublishYear,  count(authors) as NumberofAuthors from librarydata group by publish_year_new order by publish_year_new").show()

# COMMAND ----------

# Task#5: Find the number of authors and number of books published per month for years between 1950 and 1970
spark.sql("select createyear, createmonth, count(distinct authors) as authorscount from librarydata group by createyear, createmonth order by createyear, createmonth").show()

# COMMAND ----------

spark.sql("select createyear, createmonth, count(title) as bookcount from librarydata group by createyear, createmonth order by createyear, createmonth").show()

# COMMAND ----------

spark.sql("select authors,title from librarydata where title in (SELECT title FROM librarydata GROUP BY title having count(authors) >1)").show(5,truncate = False)

# COMMAND ----------


