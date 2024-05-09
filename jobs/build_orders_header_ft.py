import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def convert_date(date_str):
    date, _ = date_str.split(' ')
    return date.replace('-', '')


convert_date_udf = F.udf(convert_date)

spark = SparkSession.builder.appName('build_orders_header_ft').getOrCreate()

orders_item = spark.read.csv('s3a://olist/olist_order_items_dataset.csv', header=True)
orders_header = spark.read.csv('s3a://olist/olist_orders_dataset.csv', header=True)

orders_header_ft = orders_item.groupBy('order_id').agg(F.count('order_item_id').alias('nb_items'),
                                                       F.sum('price').alias('total_price'),
                                                       F.sum('freight_value').alias('total_freight'),
                                                       F.round(F.sum(
                                                           orders_item.price + orders_item.freight_value), 4).alias(
                                                           'total_order'))

orders_header_ft = orders_header_ft.join(orders_header, orders_header_ft.order_id == orders_header.order_id).select(
    orders_header_ft.order_id, 'nb_items',
    'total_price',
    'total_freight',
    'total_order',
    'customer_id',
    'order_purchase_timestamp')

orders_header_ft = orders_header_ft.withColumn('date_order', convert_date_udf('order_purchase_timestamp'))
orders_header_ft = orders_header_ft.drop('order_purchase_timestamp')

customers_dim = (spark.read.format('jdbc')
                 .option('url', 'jdbc:mysql://database:3306/olist')
                 .option('driver', 'com.mysql.cj.jdbc.Driver')
                 .option('dbtable', 'customers_dim')
                 .option('user', 'admin')
                 .option('password', 'admin')
                 .load())

orders_header_ft = (orders_header_ft
                    .join(customers_dim, orders_header_ft.customer_id == customers_dim.customer_id)
                    .select('order_id',
                            'nb_items',
                            'total_price',
                            'total_freight',
                            'total_order',
                            'customer_key',
                            'date_order'))

(orders_header_ft.write.format('jdbc')
 .option('url', 'jdbc:mysql://database:3306/olist')
 .option('driver', 'com.mysql.cj.jdbc.Driver')
 .option('dbtable', 'orders_header_fact')
 .option('user', 'admin')
 .option('password', 'admin')
 .mode('append')
 .save())

spark.stop()
