from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('build_products_dim').getOrCreate()

df = spark.read.csv('s3a://olist/olist_products_dataset.csv', header=True)

(df.write.format('jdbc')
 .option('url', 'jdbc:mysql://database:3306/olist')
 .option('driver', 'com.mysql.cj.jdbc.Driver')
 .option('dbtable', 'products_dim')
 .option('user', 'admin')
 .option('password', 'admin')
 .mode('append')
 .save())

spark.stop()
