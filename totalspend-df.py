from pyspark.sql import SparkSession

session: SparkSession = SparkSession.builder.appName("TotalSpend").getOrCreate()
orders_view = session.read.csv("file:///home/alessandro/projects/spark-training/customer-orders.csv").createOrReplaceTempView("customer_orders")
session.sql("SELECT _c0 AS customer_id, SUM(_c2) AS total_spend FROM customer_orders GROUP BY _c0 ORDER BY total_spend").show(10000)

