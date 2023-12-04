# -*- coding: utf-8 -*-
"""E-commerce Data Transformation.ipynb"""

#import libraries
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import count

# Connect to container
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "d23e3e97-67ba-4958-9cad-9e3fe26ffc29",
"fs.azure.account.oauth2.client.secret": 'c248Q~slEHs.S1qA86IGqhCk3_14mXbwFdVMycBM',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a97d058c-a35f-4159-ba54-efa6bfd1a32b/oauth2/token"}



dbutils.fs.mount(
source = "abfss://e-commerce-data@ecommercedatabigdata.dfs.core.windows.net", # container@storageacc
mount_point = "/mnt/e-com",
extra_configs = configs)

# Commented out IPython magic to ensure Python compatibility.
# %fs
ls "/mnt/e-comm"

#Gathering data for Transformation
e_com = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/e-comm/raw_data_e-commerce/E-commerce.csv")

e_com.show()

#Getting to know Schema
e_com.printSchema()

# Transformation of Data

# Converting to interger
e_com = e_com.withColumn("InvoiceNo",col("InvoiceNo").cast(IntegerType()))

# Removing Time part
e_com = e_com.withColumn("InvoiceDate", regexp_replace("InvoiceDate", "\\s\\d{1,2}:\\d{2}(\\s?[APMapm]{2})?$", ""))

e_com = e_com.filter(~col('Description').rlike("/"))
e_com = e_com.filter(~col('Description').rlike(""))

# Drop null values
e_com = e_com.na.drop()

e_com.printSchema()

display(e_com)

#analysis
#Top_item_sold = e_com.orderBy("Quantity", ascending = False).select("Description","Quantity").show()

#e_com1 = e_com.groupBy('Country').agg(count('Quantity').alias('Count of quantity'))
#Top_Country = e_com1.orderBy('Count of quantity', ascending=False).show()

e_com.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/e-com/transformed-data/e_commerce_transformed")