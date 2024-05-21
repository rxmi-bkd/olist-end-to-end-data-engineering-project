import pyspark.sql.functions as F

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def build_dim(dataset):
    df = spark.read.csv(f's3a://olist/{dataset}.csv', header=True)
    df = df.withColumn(dataset.split('_')[1][:-1] + '_key', F.sha(dataset.split('_')[1][:-1] + '_id'))

    df.write.format('jdbc') \
        .option('url', 'jdbc:mysql://database:3306/olist') \
        .option('driver', 'com.mysql.cj.jdbc.Driver') \
        .option('user', 'admin') \
        .option('password', 'admin') \
        .option('dbtable', dataset.split('_')[1] + '_dim') \
        .mode('append') \
        .save()


def build_facts():
    details = spark.read.csv('s3a://olist/olist_order_items_dataset.csv', header=True)
    header = spark.read.csv('s3a://olist/olist_orders_dataset.csv', header=True)
    join = details.join(header, 'order_id')
    df = join.select(details.order_id,
                     details.order_item_id,
                     F.sha(details.product_id).alias('product_key'),
                     F.sha(details.seller_id).alias('seller_key'),
                     details.price,
                     details.freight_value,
                     F.to_date(header.order_purchase_timestamp).alias('order_date'),
                     F.sha(header.customer_id).alias('customer_key'))

    df.write.format('jdbc') \
        .option('url', 'jdbc:mysql://database:3306/olist') \
        .option('driver', 'com.mysql.cj.jdbc.Driver') \
        .option('user', 'admin') \
        .option('password', 'admin') \
        .option('dbtable', 'orders_fact') \
        .mode('append') \
        .save()


build_dim('olist_sellers_dataset')
build_dim('olist_customers_dataset')
build_dim('olist_products_dataset')
build_facts()

spark.stop()
