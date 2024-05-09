import pandas as pd

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('build_date_dim').getOrCreate()

start_date = '2016-01-01'
end_date = '2018-12-31'

pd_df = pd.date_range(start=start_date, end=end_date)
pd_df = pd.DataFrame(pd_df, columns=['date'])
pd_df['day'] = pd_df['date'].dt.day
pd_df['month'] = pd_df['date'].dt.month
pd_df['year'] = pd_df['date'].dt.year
pd_df['date_key'] = pd_df['date'].dt.strftime('%Y%m%d')
pd_df = pd_df.drop(columns=['date'])

spark_df = spark.createDataFrame(pd_df)

(spark_df.write.format('jdbc')
 .option('url', 'jdbc:mysql://database:3306/olist')
 .option('driver', 'com.mysql.cj.jdbc.Driver')
 .option('dbtable', 'dates_dim')
 .option('user', 'admin')
 .option('password', 'admin')
 .mode('append')
 .save())

spark.stop()
