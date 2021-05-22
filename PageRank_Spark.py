import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

# Load the adjacency list file
AdjList1 = sc.textFile("/home/tluan/data/02AdjacencyList.txt")
print (AdjList1.collect())

AdjList2 = AdjList1.map(lambda line : line.split(" ")) 
print (AdjList2.collect())
AdjList3 = AdjList2.map(lambda x : (x[0], x[1:]))  
AdjList3.persist()
print (AdjList3.collect())

nNumOfNodes = AdjList3.count()
print ("Total Number of nodes")
print (nNumOfNodes)

# Initialize each page's rank; since we use mapValues, the resulting RDD will have the same partitioner as links
print ("Initialization")
PageRankValues = AdjList3.mapValues(lambda v : 0.2) 
print (PageRankValues.collect())

# Run 30 iterations
print ("Run 30 Iterations")
for i in range(1, 30):
    print ("Number of Iterations")
    print (i)
    JoinRDD = AdjList3.join(PageRankValues)
    print ("join results")
    print (JoinRDD.collect())
    contributions = JoinRDD.map(lambda x:x[1]).flatMap(lambda x : computeContribs(x[0],x[1])) 
    print ("contributions")
    print (contributions.collect())
    accumulations = contributions.reduceByKey(lambda x, y : x+y)  
    print ("accumulations")
    print (accumulations.collect())
    PageRankValues = accumulations.mapValues(lambda v : v*0.85 + 0.15/5)  
    print ("PageRankValues")
    print (PageRankValues.collect())

print ("=== Final PageRankValues ===")
print (PageRankValues.collect())

# Write out the final ranks
PageRankValues.coalesce(1).saveAsTextFile("/home/tluan/data/PageRankValues_Final")
