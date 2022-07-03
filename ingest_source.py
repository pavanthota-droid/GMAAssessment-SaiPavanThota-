#importing the SparkSession Module
from pyspark.sql import SparkSession

# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = r""

# Allow SPARK to read from Blob remotely
adls_source = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
  blob_sas_token)
print('Remote blob path: ' + adls_source)

#creating the spark object to read data and do transformation
spark = SparkSession.builder.appName("Assesment").getOrcreate()

# SPARK read parquet, note that it won't load any data yet by now
df = spark.read.parquet(adls_source)
target_path = "C:\Users\91957\Desktop\FullTime_Interview\Assessment"



#partition the data while reading for better performance

df.write.option("header",True) \
        .partitionBy("puYear","puMonth") \
        .mode("overwrite") \
        .parquet(target_path)






