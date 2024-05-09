import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def convert_date(date_str):
    date, _ = date_str.split(' ')
    return date.replace('-', '')


convert_date_udf = F.udf(convert_date)

spark = SparkSession.builder.appName('build_orders_detail_ft').getOrCreate()

orders_item = spark.read.csv('s3a://olist/olist_order_items_dataset.csv', header=True)
orders_header = spark.read.csv('s3a://olist/olist_orders_dataset.csv', header=True)

orders_detail_ft = (orders_item
                    .join(orders_header, 'order_id')
                    .select('order_id',
                            'product_id',
                            'seller_id',
                            'price',
                            'freight_value',
                            'order_purchase_timestamp',
                            'customer_id',
                            'order_item_id'))

orders_detail_ft = orders_detail_ft.withColumn('order_date', convert_date_udf('order_purchase_timestamp'))
orders_detail_ft = orders_detail_ft.drop('order_purchase_timestamp')

customers_dim = (spark.read.format('jdbc')
                 .option('url', 'jdbc:mysql://database:3306/olist')
                 .option('driver', 'com.mysql.cj.jdbc.Driver')
                 .option('dbtable', 'customers_dim')
                 .option('user', 'admin')
                 .option('password', 'admin')
                 .load())

products_dim = (spark.read.format('jdbc')
                .option('url', 'jdbc:mysql://database:3306/olist')
                .option('driver', 'com.mysql.cj.jdbc.Driver')
                .option('dbtable', 'products_dim')
                .option('user', 'admin')
                .option('password', 'admin')
                .load())

sellers_dim = (spark.read.format('jdbc')
               .option('url', 'jdbc:mysql://database:3306/olist')
               .option('driver', 'com.mysql.cj.jdbc.Driver')
               .option('dbtable', 'sellers_dim')
               .option('user', 'admin')
               .option('password', 'admin')
               .load())

orders_detail_ft = (orders_detail_ft
                    .join(customers_dim, 'customer_id')
                    .join(products_dim, 'product_id')
                    .join(sellers_dim, 'seller_id')
                    .select('order_id',
                            'price',
                            'freight_value',
                            'order_date',
                            'order_item_id',
                            'customer_key',
                            'product_key',
                            'seller_key'))

orders_detail_ft.show()

(orders_detail_ft.write.format('jdbc')
 .option('url', 'jdbc:mysql://database:3306/olist')
 .option('driver', 'com.mysql.cj.jdbc.Driver')
 .option('dbtable', 'orders_detail_fact')
 .option('user', 'admin')
 .option('password', 'admin')
 .mode('append')
 .save())

spark.stop()
