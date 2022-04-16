# eco_df = economy dataframe
# health_df = health dataframe
# from asyncio.windows_events import NULL
import pyspark
# import org.apache.spark.sql.expressions.Window
# from zmq import NULL
from pyspark.sql.window import Window
import sys
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col
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
eco_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
    "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+ECO_dataset)

health_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
    "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+HEALTH_dataset)

index_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
    "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+INDEX_dataset)

mon_avi_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
    "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+MONTHLY_AVIATION_dataset)

com_avi_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
    "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+COMMERCIAL_AVIATION_dataset)

vac_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(
    "wasbs://datasets@trainingbatchaccount.blob.core.windows.net/"+VACCINATIONS_dataset)
# ---------economy dataframe-----
# filling all the null values in columns  to zero except the primary_key column
eco_df = eco_df.na.fill(0)

# ---------health dataframe------
# filling the null values in the life_expectancy column with the mean of it
imputer = Imputer(inputCol='life_expectancy',
                  outputCol="life_expectancy").setStrategy("mean")
health_df = imputer.fit(health_df).transform(health_df)
# ---------index dataframe------

# index_df.printSchema()
# index_df.describe().show()
# index_df.select([count(when(col(c).isNull(), c)).alias(c) for c in index_df.columns]).show()
# # index_df.select("location_key","country_code","country_name").show()
index_df = index_df.select("location_key", "country_code",
                           "country_name", "subregion1_code", "subregion1_name")
# index_df.show()

# ---------monthly_aviation dataframe------


# converts the month from single digit to double digit
mon_avi_df = mon_avi_df.select((lpad(mon_avi_df.Month, 2, '0').alias(
    'Month')), "Year", "DOMESTIC", "INTERNATIONAL", "TOTAL")

# concats the month and year to single date column
mon_avi_df = mon_avi_df.select(F.concat_ws(
    '', mon_avi_df.Month, mon_avi_df.Year).alias('Date'), mon_avi_df["*"])

# convers the date column's type from string to date
mon_avi_df = mon_avi_df.withColumn(
    'Date', to_date(mon_avi_df['Date'], format='MMyyyy'))

# selects four major needed data columns from the data frame
mon_avi_df = mon_avi_df.select("Date", "DOMESTIC", "INTERNATIONAL", "TOTAL")
# mon_avi_df.show()

# ---------commercial_aviation dataframe------


# print ("--null values in each column commercial aviation")
# com_avi_df.select([count(when(col(c).isNull(), c)).alias(c) for c in com_avi_df.columns]).show()
# mon_avi_df = mon_avi_df.withColumn('Date',to_date(mon_avi_df['Date'],format='MMyyyy'))
# com_avi_df.printSchema()

# ---------vaccinations dataframe------

# drops the rows if all the mentions columns has null values
vac_df = vac_df.na.drop(subset=["new_persons_vaccinated","cumulative_persons_vaccinated","new_persons_fully_vaccinated","cumulative_persons_fully_vaccinated","new_vaccine_doses_administered","cumulative_vaccine_doses_administered"] ,how="all")

vac_df = vac_df.select("date",	"location_key",	"new_persons_vaccinated","cumulative_persons_vaccinated")

print("--null values in each column commercial aviation")
vac_df.select([count(when(col(c).isNull(), c)).alias(c)for c in vac_df.columns]).show()
# mon_avi_df = mon_avi_df.withColumn('Date',to_date(mon_avi_df['Date'],format='MMyyyy'))

# vac_df.show()

# vac_df.withColumn("new_colm", F.last('id', True).over(Window.partitionBy('location_key').orderBy('ts').rowsBetween(-sys.maxsize, 0))).show()
vac_df.withColumn("new_colm", F.last('cumulative_persons_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0))).show()
vac_df.withColumn("new_colm1", F.last('cumulative_persons_fully_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0))).show()
vac_df.withColumn("new_colm2", F.last('cumulative_vaccine_doses_administered', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0))).show()

# condition = F.lag(F.col("cumulative_persons_vaccinated"), 1).isNull()

# vac_df.withColumn("lag",F.lag("ID",1).over(F.windowSpec)).show()

# vac_df = vac_df.withColumn("cleaned_LK", F.when(condition, F.col(111111111)))

# vac_df.show()
print("------------after cleaning selected column null-")
vac_df.select([count(when(col(c).isNull(), c)).alias(c)for c in vac_df.columns]).show()
