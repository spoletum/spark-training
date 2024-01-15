import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

splitter = re.compile(r'\W+', re.UNICODE)

config = SparkConf().setMaster("local").setAppName("AverageFriends").set("spark.log.level", "ERROR")
sc = SparkContext(conf = config)

source = sc.textFile("file:///home/alessandro/projects/spark-training/Book")

results = source.flatMap(lambda l: splitter.split(l.lower())).map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda w: w[1]).collect()

for result in results:
    print("{}\t{}".format(result[0], result[1]))