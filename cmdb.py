from pyspark.sql import SparkSession, Row

spark: SparkSession = SparkSession.builder.appName("TotalSpend").config("spark.log.level", "WARN").getOrCreate()
orders_view = spark.read.option("header", "true").csv("file:///home/alessandro/projects/spark-training/cmdb.csv").createOrReplaceTempView("cmdb")
spark.sql("SELECT SUM(`Total CPUs` * `Cores Per CPU`) AS core_count FROM cmdb WHERE uuid IS NOT NULL AND `In Service` = true AND `Service Level` = 'Production' AND `Location` LIKE 'Bettembourg%'").show()
spark.stop()