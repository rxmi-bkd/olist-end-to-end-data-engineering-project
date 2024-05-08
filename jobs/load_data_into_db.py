from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('load_data_into_db').getOrCreate()

columns = ["id", "name", "age", "gender"]
data = [(1, "James", 30, "M"), (2, "Ann", 40, "F"), (3, "Jeff", 41, "M"), (4, "Jennifer", 20, "F")]
df = spark.createDataFrame(data, columns)

df.write.format("jdbc").option("url", "jdbc:mysql://database:3306/olist").option("driver",
                                                                                 "com.mysql.cj.jdbc.Driver").option(
    "dbtable", "users").option("user", "admin").option("password", "admin").save()

spark.stop()
