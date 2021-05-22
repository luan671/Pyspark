import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql.session import SparkSession

#Initializing a pyspark session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#load the .json file as data frame
tweetDF = spark.read.json("/home/tluan/data/tweets.json")
mapDF = spark.read.json("/home/tluan/data/cityStateMap.json")

#isolate the geometric data from tweet data frame
DF1=tweetDF.select("geo")
#change the column name from "geo" to "city" for easier comparison in next step
df1=DF1.selectExpr("geo as city")
#use join-left feature in pyspark to compelement all cities with their
#corresponding state name from mapDF
df2=df1.join(mapDF, on=['city'], how='left')
#group all attributes in the "state" column
df3=df2.groupby('state')
#use count() function to generate the count for all states
df3.count().show()

