import airflow

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

sql = '''
use olist;

drop table if exists orders_fact;
drop table if exists sellers_dim;
drop table if exists customers_dim;
drop table if exists products_dim;

create table sellers_dim
(
    seller_id              varchar(255) not null,
    seller_zip_code_prefix varchar(255) null,
    seller_city            varchar(255) null,
    seller_state           varchar(255) null,
    seller_key             varchar(255) primary key
);

create table customers_dim
(
    customer_id              varchar(255) not null,
    customer_unique_id       varchar(255) null,
    customer_zip_code_prefix varchar(255) null,
    customer_city            varchar(255) null,
    customer_state           varchar(255) null,
    customer_key             varchar(255) primary key
);

create table products_dim
(
    product_id                 varchar(255) not null,
    product_category_name      varchar(255) null,
    product_name_lenght        int          null,
    product_description_lenght int          null,
    product_photos_qty         int          null,
    product_weight_g           int          null,
    product_length_cm          int          null,
    product_height_cm          int          null,
    product_width_cm           int          null,
    product_key                varchar(255) primary key
);

create table orders_fact
(
    order_id      varchar(255),
    order_item_id int,
    product_key   varchar(255),
    seller_key    varchar(255),
    price         double,
    freight_value double,
    order_date    date,
    customer_key  varchar(255),
    primary key (order_id, order_item_id),
    foreign key (product_key) references products_dim (product_key),
    foreign key (seller_key) references sellers_dim (seller_key),
    foreign key (customer_key) references customers_dim (customer_key)
);
'''
dag = DAG(dag_id='build_olist_orders_ft',
          description='Build Olist orders fact table using Spark',
          default_args={'owner': 'rami boukadida', 'start_date': airflow.utils.dates.days_ago(1)},
          schedule_interval='@once')

conf = {'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'adminadmin',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'}

build_dw_schema = SQLExecuteQueryOperator(task_id='build_dw_schema',
                                          conn_id='mysql-conn',
                                          sql=sql,
                                          dag=dag)

build_orders_fact_table = SparkSubmitOperator(task_id='build_orders_fact_table',
                                          conn_id='spark-conn',
                                          application='/opt/bitnami/airflow/jobs/orders_fact_table.py',
                                          dag=dag,
                                          jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                          conf=conf)

build_dw_schema >> build_orders_fact_table
