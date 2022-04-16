import pyspark
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession

STORAGEACCOUNTURL = "https://trainingbatchaccount.blob.core.windows.net"
STORAGEACCOUNTKEY = "2QPPHsAtQ8/fh33VE7wqg/ZaeJoxdq/pnevAEmCh0n32tC5eXa8dTEEwMHdD9Ff5k1/wVh97aubqgKzQSwOLnQ=="
CONTAINERNAME = "datasets"
ECONOMY = "economy.csv"
INDEX = "index.csv"


spark = SparkSession.builder.appName('azure').getOrCreate()
spark.conf.set(
        "fs.azure.account.key.trainingbatchaccount.blob.core.windows.net",
        STORAGEACCOUNTKEY
)
eco_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+ECONOMY)
index_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/index.csv")
# eco_df.show()
# index_df.printSchema()# index_df.printSchema()
# index_df.show()


#   empDF.join(deptDF,empDF("emp_dept_id") ==  deptDF("dept_id"),"left")
#     .show(false)
#   empDF.join(deptDF,empDF("emp_dept_id") ==  deptDF("dept_id"),"leftouter")
#     .show(false)
eco_df=eco_df.select(["location_key","gdp_usd","gdp_per_capita_usd","human_capital_index"])

index_df=index_df.select(["location_key","country_name"])

eco_df.join(index_df,eco_df.location_key ==  index_df.location_key,"leftouter").show()
# .show(false)'location_key'