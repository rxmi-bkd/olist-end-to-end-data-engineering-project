import airflow

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

sql = '''
use
olist;

drop table if exists orders_header_fact;
drop table if exists orders_detail_fact;
drop table if exists sellers_dim;
drop table if exists customers_dim;
drop table if exists dates_dim;
drop table if exists products_dim;

create table sellers_dim
(
    seller_id              varchar(255) null,
    seller_zip_code_prefix varchar(255) null,
    seller_city            varchar(255) null,
    seller_state           varchar(255) null,
    seller_key             int auto_increment primary key
);

create table customers_dim
(
    customer_id              varchar(255) null,
    customer_unique_id       varchar(255) null,
    customer_zip_code_prefix varchar(255) null,
    customer_city            varchar(255) null,
    customer_state           varchar(255) null,
    customer_key             int auto_increment primary key
);

create table dates_dim
(
    day      int        not null,
    month    int        not null,
    year     int        not null,
    date_key varchar(8) not null primary key
);

create table products_dim
(
    product_id                 varchar(255) null,
    product_category_name      varchar(255) null,
    product_name_lenght        int null,
    product_description_lenght int null,
    product_photos_qty         int null,
    product_weight_g           int null,
    product_length_cm          int null,
    product_height_cm          int null,
    product_width_cm           int null,
    product_key                int auto_increment primary key
);

create table orders_header_fact
(
    nb_items     int null,
    total_price double null,
    total_freight double null,
    total_order double null,
    customer_key int null,
    date_order   varchar(8),
    order_id     varchar(255),
    primary key (order_id),
    foreign key (date_order) references dates_dim (date_key),
    foreign key (customer_key) references customers_dim (customer_key)
);

create table orders_detail_fact
(
    price double null,
    freight_value double null,
    order_date    varchar(8) null,
    customer_key  int,
    product_key   int,
    seller_key    int,
    order_item_id int,
    order_id      varchar(255),
    primary key (customer_key, seller_key, product_key, order_id, order_item_id),
    foreign key (customer_key) references customers_dim (customer_key),
    foreign key (product_key) references products_dim (product_key),
    foreign key (seller_key) references sellers_dim (seller_key)
);
'''
dag = DAG(dag_id='build_olist_orders_ft',
          description='Build Olist orders fact table using Spark',
          default_args={'owner': 'rami boukadida', 'start_date': airflow.utils.dates.days_ago(1)},
          schedule_interval='@daily')

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

build_customers_dim = SparkSubmitOperator(task_id='build_customers_dim',
                                          conn_id='spark-conn',
                                          application='/opt/bitnami/airflow/jobs/build_customers_dim.py',
                                          dag=dag,
                                          jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                          conf=conf)

build_dates_dim = SparkSubmitOperator(task_id='build_dates_dim',
                                      conn_id='spark-conn',
                                      application='/opt/bitnami/airflow/jobs/build_dates_dim.py',
                                      dag=dag,
                                      jars='/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar')

build_sellers_dim = SparkSubmitOperator(task_id='build_sellers_dim',
                                        conn_id='spark-conn',
                                        application='/opt/bitnami/airflow/jobs/build_sellers_dim.py',
                                        dag=dag,
                                        jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                        conf=conf)

build_products_dim = SparkSubmitOperator(task_id='build_products_dim',
                                         conn_id='spark-conn',
                                         application='/opt/bitnami/airflow/jobs/build_products_dim.py',
                                         dag=dag,
                                         jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                         conf=conf)

build_orders_header_ft = SparkSubmitOperator(task_id='build_orders_header_ft',
                                             conn_id='spark-conn',
                                             application='/opt/bitnami/airflow/jobs/build_orders_header_ft.py',
                                             dag=dag,
                                             jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                             conf=conf)

build_orders_detail_ft = SparkSubmitOperator(task_id='build_orders_detail_ft',
                                             conn_id='spark-conn',
                                             application='/opt/bitnami/airflow/jobs/build_orders_detail_ft.py',
                                             dag=dag,
                                             jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                             conf=conf)

build_dw_schema >> build_customers_dim >> build_dates_dim >> build_sellers_dim >> build_products_dim >> build_orders_header_ft >> build_orders_detail_ft
