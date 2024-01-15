from pyspark import SparkConf, SparkContext

config = SparkConf().setMaster("local").setAppName("TotalSpend")
sc = SparkContext(conf = config)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    price = float(fields[2])
    return (customerId, price)

def sortByCustomerId(v):
    return v[0]

def sortByTotalSpend(v):
    return v[1]

def addToTotalSpend(a, v):
    return a + v

lines = sc.textFile("file:///home/alessandro/projects/spark-training/customer-orders.csv")
results = lines.map(parseLine).reduceByKey(addToTotalSpend).sortBy(sortByTotalSpend).collect()

for result in results:
    print("{:<10}\t${:>10.2f}".format(result[0], result[1]))
