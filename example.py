
import pyspark
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType
from pyspark.sql import functions as F

STORAGEACCOUNTURL = "https://trainingbatchaccount.blob.core.windows.net"
STORAGEACCOUNTKEY = "2QPPHsAtQ8/fh33VE7wqg/ZaeJoxdq/pnevAEmCh0n32tC5eXa8dTEEwMHdD9Ff5k1/wVh97aubqgKzQSwOLnQ=="
CONTAINERNAME = "datasets"
BLOBNAME = "economy.csv"

# def changeDate(dateString):

#  months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
#  for i,s in enumerate(months):
#   if(dateString[0:3] == s):
#    if(i<9):
#     return '01-0'+str(i+1)+'-'+dateString[4:]
#    else:
#     return '01-'+str(i+1)+'-'+dateString[4:]
#   else:
#    return '0'

# cd_udf = F.udf(changeDate, StringType())

spark = SparkSession.builder.appName('azure').getOrCreate()
spark.conf.set(
        "fs.azure.account.key.trainingbatchaccount.blob.core.windows.net",
        STORAGEACCOUNTKEY
)
sdf = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/index.csv")

#print(type(F.col(sdf.Date)))
# sdf = sdf.withColumn('Date',cd_udf('Date'))
# sdf = sdf.withColumn('Date',F.to_date(sdf['Date'],'dd-mm-yyyy'))
sdf.show()
sdf.printSchema()

