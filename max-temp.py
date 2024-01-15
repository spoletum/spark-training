from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

config = SparkConf().setMaster("local").setAppName("AverageFriends").set("spark.log.level", "ERROR")
sc = SparkContext(conf = config)

source = sc.textFile("file:///home/alessandro/projects/spark-training/1800.csv")

results = source.map(parseLine).filter(lambda x: "TMAX" in x[1]).map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x, y)).collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))