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
eco_df = spark.read.format('csv').option('header',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+ECONOMY.csv)
index_df = spark.read.format('csv').option('header',True).load("wasbs://datasets@trainingbatchaccount.blobcore.windows.net/"+INDEX.csv)
eco_df.show()
index_df.show()

