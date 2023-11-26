from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("ReduceExample")
sc = SparkContext(conf = conf)

source = sc.parallelize([1, 2, 3, 4, 5])
result = source.reduce(lambda a, v: a * v)

print("Final result: " + str(result))