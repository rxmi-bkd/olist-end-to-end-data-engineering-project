from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read_data_from_db').getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:mysql://database:3306/olist").option("driver",
                                                                                 "com.mysql.cj.jdbc.Driver").option(
    "dbtable", "users").option("user", "admin").option("password", "admin").load()

df.show()

spark.stop()
