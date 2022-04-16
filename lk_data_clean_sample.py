# eco_df = economy dataframe 
# check_df = local dataframe for checking purpose

import pyspark
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when,col

STORAGEACCOUNTURL = "https://trainingbatchaccount.blob.core.windows.net"
STORAGEACCOUNTKEY = "2QPPHsAtQ8/fh33VE7wqg/ZaeJoxdq/pnevAEmCh0n32tC5eXa8dTEEwMHdD9Ff5k1/wVh97aubqgKzQSwOLnQ=="
CONTAINERNAME = "datasets"
ECO_dataset= "economy.csv"



spark = SparkSession.builder.appName('azure').getOrCreate()
spark.conf.set(
        "fs.azure.account.key.trainingbatchaccount.blob.core.windows.net",
        STORAGEACCOUNTKEY
)
eco_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+ECO_dataset)
print("------------------count of null values before data cleaning-----------------")
eco_df.select([count(when(col(c).isNull(), c)).alias(c) for c in eco_df.columns]).show()

eco_df.printSchema()
# checking if all the three columns  has null values except primary_key column
check_df=eco_df.na.drop(subset=['gdp_usd','gdp_per_capita_usd','human_capital_index'] ,how="all")
# check_df.describe().show()

# note: since the above condition is false filled the null vales to zero
# filling all the null values in columns  to zero except the primary_key column
eco_df=eco_df.na.fill(0)

eco_df.describe().show()

print("------------------count of null values after data cleaning-----------------")
eco_df.select([count(when(col(c).isNull(), c)).alias(c) for c in eco_df.columns]).show()
