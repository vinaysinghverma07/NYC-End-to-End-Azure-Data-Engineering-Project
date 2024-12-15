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

# MAGIC %md
# MAGIC # **Database Creation**

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog "yourcatalogname";
# MAGIC --DROP SCHEMA IF EXISTS gold CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS gold ;

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading and Writing and Creating delta tables.**

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC **Storage Variables**

# COMMAND ----------

silver = 'abfss://silver@yourstorageaccount.dfs.core.windows.net'
gold = 'abfss://gold@yourstorageaccount.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Zone**

# COMMAND ----------

df_zone = spark.read.format('parquet')\
                .option('inferSchema', True)\
                .option('header',True)\
                .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format('delta')\
                .mode('append')\
                .option('path', f'{gold}/trip_zone')\
                .saveAsTable('gold.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.trip_zone
# MAGIC where borough = 'EWR';

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

df_trip_type = spark.read.format('parquet')\
                .option('inferSchema', True)\
                .option('header',True)\
                .load(f'{silver}/trip_type')

# COMMAND ----------

df_trip_type.write.format('delta')\
                .mode('append')\
                .option('path',f'{gold}/trip_type')\
                .saveAsTable('gold.trip_type')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.trip_type;

# COMMAND ----------

# MAGIC %md
# MAGIC **Trips Data**

# COMMAND ----------

df_trip_data = spark.read.format('parquet')\
                .option('inferSchema', True)\
                .option('header',True)\
                .load(f'{silver}/trips2023data')

# COMMAND ----------

df_trip_data.display()

# COMMAND ----------

df_trip_data.write.format('delta')\
                .mode('append')\
                .option('path',f'{gold}/trips2023data')\
                .saveAsTable('gold.trip_trip')

# COMMAND ----------

# MAGIC %md
# MAGIC **Learning Delta lake**

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ;
# MAGIC Select * from gold.trip_zone
# MAGIC where locationID='1';

# COMMAND ----------

# MAGIC %sql
# MAGIC update gold.trip_zone
# MAGIC set borough = 'EMR'
# MAGIC where locationID = '1';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ;
# MAGIC Select * from gold.trip_zone
# MAGIC where locationID='1';

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from gold.trip_zone
# MAGIC where locationID = '1'

# COMMAND ----------

# MAGIC %sql
# MAGIC update gold.trip_zone
# MAGIC set borough = 'EMR'
# MAGIC where locationID = '1';
# MAGIC
# MAGIC use catalog hive_metastore;
# MAGIC Select * from gold.trip_zone
# MAGIC where locationID='1';

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE history gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;
# MAGIC Select * from gold.trip_zone
# MAGIC where locationID='1';

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 0 means when the actual was created
# MAGIC RESTORE gold.trip_zone to version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;
# MAGIC Select * from gold.trip_zone
# MAGIC where locationID='1';

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC **Tripe Zone**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.trip_trip