from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create sparksession object
spark = SparkSession.builder.master('local').appName("test").getOrCreate()

# reading data
dflibdata= spark.read.load("file:\\c:\dataset\\ol_cdump.json", format = 'json')

# Total number of records in raw dataset
dfcount = dflibdata.agg(count(col("*")).alias("Total Number of Records"))
dfcount.show()

# Get familiar with schema
# Can see we have few struct fields in the dataset
dflibdata.printSchema()

# Data Cleaning#1: extracting a subset of dataset with only fields required for our analysis
dflibdata1_1= dflibdata.select(dflibdata.authors, dflibdata.genres, dflibdata.number_of_pages, dflibdata.publish_date, dflibdata.publishers , dflibdata.title,dflibdata.created.value)
dflibdata1 = dflibdata1_1.withColumnRenamed("created.value","created")
dflibdata1.show()

# Data Cleaning#2: drop duplicates
dflibdata2 = dflibdata1.dropDuplicates()

# Data Cleaning#3: drop records with NULL values
dflibdata3 = dflibdata2.filter(dflibdata2.authors.isNotNull()).filter(dflibdata2.number_of_pages.isNotNull())\
    .filter(dflibdata2.publish_date.isNotNull()).filter(dflibdata2.title.isNotNull()).filter(dflibdata2.created.isNotNull())

# Data Cleaning # 4: Genres is an array field so cleaning and fetching the genres in a new column out of this array element
# so exploding this array element to have multiple rows for each of the array element under genres field
dflibdata4 = dflibdata3.filter(dflibdata3.genres.isNotNull())
dflibdata5 = dflibdata4.select(dflibdata4.authors, dflibdata4.genres,dflibdata4.number_of_pages,dflibdata4.publish_date, \
                               dflibdata4.publishers , dflibdata4.title,dflibdata4.created)

# Data Cleaning#5: Publish_date:
# this date is of type STRING and having data in various format as mentioned below:
# 23 Sept 2010, 2012 etc
# and also have nulls
# Fetch the year from this field into a new field having year data.
# ignored all the nulls and
# all those records where publish_year is before 1950
dflibdata6 = dflibdata5.withColumn("publish_year_new",substring(dflibdata5.publish_date,-4,4).cast('Int'))
dflibdata7 = dflibdata6.filter(dflibdata6.publish_year_new > 1950)
# validate that now the dataset do not have any record with publish_year less than 1950.
dflibdata7.select(min(dflibdata7.publish_year_new)).show()


# Data Cleaning#6: number_of_pages
# we have records present having number_of_pages less than 20 and also few records with null values
# so filter out such records
dflibdata8 = dflibdata7.filter(dflibdata7.number_of_pages > 20)
dflibdata9 = dflibdata8.filter(dflibdata8.number_of_pages.isNotNull())
# Validate that after this data cleaning, we do not have any record with number_of_pages less than 20 or with NULL
dflibdata9.select (min(dflibdata9.number_of_pages)).show()
dflibdata9.where (dflibdata9.number_of_pages.isNull()).agg(count(col("*")).alias("number of records with NULL")).show()

# Data Cleaning#7: Title field
# we have records having title as NULL and also few records with title as blank
# filter out such records
dflibdata10 = dflibdata9.filter(dflibdata9.title.isNotNull())
dflibdata11 = dflibdata10.filter(dflibdata10.title != "")
# Validat that after the data cleaning, we do not have any record in dataset with title as NULL or blank
dflibdata11.where (dflibdata11.title.isNull()).agg(count(col("*")).alias("number of records with NULL")).show()
dflibdata11.where (dflibdata11.title == "").agg(count(col("*")).alias("number of records with blank title")).show()

# Derive new columns month, year from created column which are required for req#5
yearrange = (1950, 2021)
dflibdata12 = dflibdata11.withColumn("createtime",to_timestamp(col("created")))
dflibdata13 = dflibdata12.withColumn("createdate",to_date(col("created")))
dflibdata14 = dflibdata13.withColumn("createmonth",month(col("createdate")))
dflibdata15 = dflibdata14.withColumn("createyear",year(col("createdate")))
dflibdata16 = dflibdata15.filter(col("createyear").between(*yearrange))
dflibdata16.show(100,truncate=False)
dflibdata16count = dflibdata16.agg(count(col("*")).alias("Total Number of Records after Data Cleaning"))
dflibdata16count.show()

dflibdata17 = dflibdata16.select(explode(dflibdata16.genres).alias("newgenres"),dflibdata16.title)

#Now we have he cleaned dataset to run our analytics
dflibdata16.createOrReplaceTempView("librarydata")
dflibdata17.createOrReplaceTempView("librarydatagenres")
spark.sql("select count(*) from librarydata").show()

dflibdata11.printSchema()
dflibdata11.show(100, truncate= False)

# Task#1: Get the book with the most pages
print("Task#1:")
spark.sql("select title,number_of_pages from librarydata where number_of_pages = (select max(number_of_pages) from librarydata )").show(truncate=False)


# Task#2: Find the top 5 genres with most books
print("Task#2:")
spark.sql("select newgenres, titlecount from (select newgenres, count(title) as titlecount from librarydatagenres group by newgenres) a order by titlecount desc limit 5").show(truncate=False)



# Task#3: Retrieve the top 5 authors who (co-)authored the most books
print("Task#3:")
spark.sql("select authors,title from librarydata where title in (SELECT title FROM librarydata GROUP BY title having count(authors) >1)").show(5,truncate=False)


# Task#4: Per publish year, get the number of authors that published at least one book.
print("Task#4:")
spark.sql("select publish_year_new as PublishYear,  count(authors) as NumberofAuthors from librarydata group by publish_year_new order by publish_year_new").show()



# Task#5: Find the number of authors and number of books published per month for years between 1950 and 1970
print("Task#5:")
spark.sql("select createyear, createmonth, count(distinct authors) as authorscount from librarydata group by createyear, createmonth order by createyear, createmonth").show()

spark.sql("select createyear, createmonth, count(title) as bookcount from librarydata group by createyear, createmonth order by createyear, createmonth").show()


