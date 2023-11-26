from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

schema = StructType([
    StructField("Number", IntegerType(), True),
    StructField("Count", IntegerType(), True)
])

config = SparkConf().setMaster("local").setAppName("AverageFriends").set("spark.log.level", "ERROR")
context = SparkContext(conf = config)

source = context.parallelize([1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3])
result = source.map(lambda x: (x, 1)).countByKey()

print(result)