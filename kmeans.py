import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

#Initializing a pyspark session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#read input into dataset
dataset = spark.read.format("libsvm").load("/home/tluan/data/kmeans_input.txt")

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)