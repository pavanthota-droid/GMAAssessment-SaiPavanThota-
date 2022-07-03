#importing the SparkSession Module
from pyspark.sql import SparkSession

#reading the data from the local machine
local_path = "C:\Users\91957\Desktop\FullTime_Interview\Assessment"

#creating the spark object to read data and do transformations
spark = SparkSession.builder.appName("Assesment").getOrcreate()

df = spark.read.parquet(local_path)


# SPARK read parquet, note that it won't load any data yet by now
print('Register the DataFrame as a SQL temporary view: source')
df.createOrReplaceTempView('source')

#Transformation to get the avg cost, price, count of passengers by year and month 
df_new = spark.sql('SELECT puYear,puMonth,paymentType AVG(TotalAmount) as avg_cost, AVG(fareAmount) as avg_price, sum(passenegerCount) as  total_passeneger_counts group by puYear,puMonth,paymentType \
                    order by puYear,puMonth,paymentType DESC')


#Reading the transformed data in to a parquet file along with the header
df_new.write.option("header",True) \
        .mode("overwrite") \
        .parquet(target_path)








