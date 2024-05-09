from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read_data_from_dw').getOrCreate()

df = spark.read.csv('s3a://olist/olist_customers_dataset.csv', header=True)

print(df.show(5))

spark.stop()
