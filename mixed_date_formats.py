import pyspark
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession

STORAGEACCOUNTURL = "https://trainingbatchaccount.blob.core.windows.net"
STORAGEACCOUNTKEY = "2QPPHsAtQ8/fh33VE7wqg/ZaeJoxdq/pnevAEmCh0n32tC5eXa8dTEEwMHdD9Ff5k1/wVh97aubqgKzQSwOLnQ=="
CONTAINERNAME = "datasets"
CAD_dataset= "Commercial_Aviation_Departures.csv"



spark = SparkSession.builder.appName('azure').getOrCreate()
spark.conf.set(
        "fs.azure.account.key.trainingbatchaccount.blob.core.windows.net",
        STORAGEACCOUNTKEY
)
CAD_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+CAD_dataset)

# eco_df.show()
# index_df.printSchema()# index_df.printSchema()
# index_df.show()


#   empDF.join(deptDF,empDF("emp_dept_id") ==  deptDF("dept_id"),"left")
#     .show(false)
#   empDF.join(deptDF,empDF("emp_dept_id") ==  deptDF("dept_id"),"leftouter")
#     .show(false)
CAD_df=CAD_df.select(["Date","Lowest","Current","Last Year"])
CAD_df.printSchema()

CAD_df.describe().show()
# CAD_df.show()
# .show(false)'location_key'