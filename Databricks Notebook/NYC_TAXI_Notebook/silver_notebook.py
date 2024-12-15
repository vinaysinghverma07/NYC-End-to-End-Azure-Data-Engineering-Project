# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

# Please replace your below configurations before running this below databricks notebook
# yourstorageaccount, yourapplicationID, yoursecretkeygenerated, yourtenenatID


spark.conf.set("fs.azure.account.auth.type.yourstorageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.yourstorageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.yourstorageaccount.dfs.core.windows.net", "yourapplicationID")
spark.conf.set("fs.azure.account.oauth2.client.secret.yourstorageaccount.dfs.core.windows.net", "yoursecretkeygenerated")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.yourstorageaccount.dfs.core.windows.net", "https://login.microsoftonline.com/yourtenenatID/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@yourstorageaccount.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ### ## Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC **"""Reading CSV Data"""**

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type Data**

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/trip_type')


# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip = spark.read.format('parquet')\
            .schema(myschema)\
            .option('header',True)\
            .option('recursiveFileLookup',True)\
            .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/trips2023data')

# COMMAND ----------

myschema = '''
                VendorID BIGINT,
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag STRING,
                RateCodeID BIGINT,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                passenger_count BIGINT,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                imporvement_surcharge DOUBLE,
                total_amount DOUBLE,
                payment_type BIGINT,
                trip_type BIGINT,
                congestion_surcharge DOUBLE
        
    '''


# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data Transformation**

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi Trip Type Data**

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('description', 'trip_description')
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
    .mode('append')\
    .option('header',True)\
    .option("path", "abfss://silver@yourstorageaccount.dfs.core.windows.net/trip_type")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------


df_trip_zone = df_trip_zone.withColumn('zone1', split(col('zone'),'/')[0])\
                            .withColumn('zone2', split(col('zone'),'/')[1])

df_trip_zone.display()




# COMMAND ----------

df_trip_zone.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@yourstorageaccount.dfs.core.windows.net/trip_zone")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip = df_trip.withColumn('lpep_pickup_datetime',to_date('lpep_pickup_datetime'))\
                .withColumn('trip_year',year('lpep_pickup_datetime'))\
                .withColumn('trip_month',month('lpep_pickup_datetime'))
df_trip.display()

# COMMAND ----------

df_trip = df_trip.select('VendorID','PULocationID','DOLocationID','fare_amount','total_amount')
df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@yourstorageaccount.dfs.core.windows.net/trips2023data")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Count Rows in Both Layers:**

# COMMAND ----------

#bronze_df = spark.read.parquet("abfss://bronze@yourstorageaccount.dfs.core.windows.net/trips2023data")
#silver_df = spark.read.format("parquet").load("abfss://silver@yourstorageaccount.dfs.core.windows.net/trips2023data/")
#print("Bronze Layer Row Count:", bronze_df.count())
#print("Silver Layer Row Count:", silver_df.count())
