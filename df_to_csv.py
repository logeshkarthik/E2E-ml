import os 
import pandas as pd
import pyspark
from pyspark.sql.window import Window
import sys
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when,col,expr, udf, avg,to_date,regexp_replace,last,lpad,concat_ws,date_format,year,month
from pyspark.sql.functions import mean, min, max, to_date
from pyspark.ml.feature import Imputer
from pyspark.sql import functions as F
from pyspark.sql.functions import lpad
# from pyspark.sql.functions import count, when,col,expr, udf, avg

STORAGEACCOUNTURL = "https://trainingbatchaccount.blob.core.windows.net"
STORAGEACCOUNTKEY = "2QPPHsAtQ8/fh33VE7wqg/ZaeJoxdq/pnevAEmCh0n32tC5eXa8dTEEwMHdD9Ff5k1/wVh97aubqgKzQSwOLnQ=="
CONTAINERNAME = "datasets"
ECO_dataset = "economy.csv"
HEALTH_dataset = "health.csv"
INDEX_dataset = "index.csv"
MONTHLY_AVIATION_dataset = "monthly_aviation.csv"
COMMERCIAL_AVIATION_dataset = "Commercial_Aviation_Departures.csv"
VACCINATIONS_dataset = "vaccinations.csv"


spark = SparkSession.builder.appName('azure').getOrCreate()
spark.conf.set(
    "fs.azure.account.key.trainingbatchaccount.blob.core.windows.net",
    STORAGEACCOUNTKEY
)
# eco_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
#     "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+ECO_dataset)

# health_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
#     "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+HEALTH_dataset)

# index_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
#     "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+INDEX_dataset)

# mon_avi_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
#     "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+MONTHLY_AVIATION_dataset)

# com_avi_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
#     "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+COMMERCIAL_AVIATION_dataset)

# vac_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
#     "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+VACCINATIONS_dataset)


# ---------vaccinations dataframe------

# # drops the rows if all the mentions columns has null values
# vac_df = vac_df.na.drop(subset=["new_persons_vaccinated","cumulative_persons_vaccinated","new_persons_fully_vaccinated","cumulative_persons_fully_vaccinated","new_vaccine_doses_administered","cumulative_vaccine_doses_administered"] ,how="all")

# vac_df = vac_df.select("date",	"location_key",	"new_persons_vaccinated","cumulative_persons_vaccinated")

# print("--null values in each column commercial aviation")
# vac_df.select([count(when(col(c).isNull(), c)).alias(c)for c in vac_df.columns]).show()
# # mon_avi_df = mon_avi_df.withColumn('Date',to_date(mon_avi_df['Date'],format='MMyyyy'))

# # vac_df.show()

# # vac_df.withColumn("new_colm", F.last('id', True).over(Window.partitionBy('location_key').orderBy('ts').rowsBetween(-sys.maxsize, 0))).show()
# vac_df.withColumn("new_colm", F.last('cumulative_persons_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0))).show()
# vac_df.withColumn("new_colm1", F.last('cumulative_persons_fully_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0))).show()
# vac_df.withColumn("new_colm2", F.last('cumulative_vaccine_doses_administered', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0))).show()

# # condition = F.lag(F.col("cumulative_persons_vaccinated"), 1).isNull()

# # vac_df.withColumn("lag",F.lag("ID",1).over(F.windowSpec)).show()

# # vac_df = vac_df.withColumn("cleaned_LK", F.when(condition, F.col(111111111)))

# # vac_df.show()
# print("------------after cleaning selected column null-")
# vac_df.select([count(when(col(c).isNull(), c)).alias(c)for c in vac_df.columns]).show()

#-----------------------------------Vaccination------------------------------------------------------------------
#Reading vaccination data
df_vaccination = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/vaccinations.csv")

#selecting needed columns only
df_vaccination = df_vaccination.select("date","location_key","new_persons_vaccinated","cumulative_persons_vaccinated","new_persons_fully_vaccinated","cumulative_persons_fully_vaccinated","new_vaccine_doses_administered","cumulative_vaccine_doses_administered")

#Drop rows if all the values are null
df_vaccination = df_vaccination.na.drop(subset=["new_persons_vaccinated","cumulative_persons_vaccinated","new_persons_fully_vaccinated","cumulative_persons_fully_vaccinated","new_vaccine_doses_administered","cumulative_vaccine_doses_administered"] ,how="all")

#fill cumulative value with previous field value (Forward Fill)
df_vaccination = df_vaccination.withColumn("cumulative_persons_vaccinated", last('cumulative_persons_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0)))
df_vaccination = df_vaccination.withColumn("cumulative_persons_fully_vaccinated",last('cumulative_persons_fully_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0)))
df_vaccination = df_vaccination.withColumn("cumulative_vaccine_doses_administered",last('cumulative_vaccine_doses_administered', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0)))

#replace null with zero
df_vaccination = df_vaccination.na.fill(value=0)

#correct date format
df_vaccination = df_vaccination.withColumn('Date',to_date(df_vaccination['Date'],format='yyyy-mm-dd'))
df_vaccination.show()
# df_vaccination.write.csv('./x.csv')
# df3.write.csv("output")
# df_vaccination.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))

# pd_data = pd.DataFrame(df_vaccination)
df_vaccination.toPandas().to_csv("xyz.csv")
# pd_data = pd.DataFrame(df_vaccination)
# file_name = 'pd_data.xlsx'
# pd_data.to_excel(file_name)
# print('DataFrame is written to Excel File successfully.')