import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(dag_id='build_olist_orders_ft',
          description='Build Olist orders fact table using Spark',
          default_args={'owner': 'rami boukadida', 'start_date': airflow.utils.dates.days_ago(1)},
          schedule_interval='@daily')

build_customers_dim = SparkSubmitOperator(task_id='build_customers_dim',
                                          conn_id='spark-conn',
                                          application='/opt/bitnami/airflow/jobs/build_customers_dim.py',
                                          dag=dag,
                                          jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                          conf={'spark.hadoop.fs.s3a.access.key': 'admin',
                                                'spark.hadoop.fs.s3a.secret.key': 'adminadmin',
                                                'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
                                                'spark.hadoop.fs.s3a.path.style.access': 'true',
                                                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
                                                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'})

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
                                        conf={'spark.hadoop.fs.s3a.access.key': 'admin',
                                              'spark.hadoop.fs.s3a.secret.key': 'adminadmin',
                                              'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
                                              'spark.hadoop.fs.s3a.path.style.access': 'true',
                                              'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
                                              'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'})

build_products_dim = SparkSubmitOperator(task_id='build_products_dim',
                                         conn_id='spark-conn',
                                         application='/opt/bitnami/airflow/jobs/build_products_dim.py',
                                         dag=dag,
                                         jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                         conf={'spark.hadoop.fs.s3a.access.key': 'admin',
                                               'spark.hadoop.fs.s3a.secret.key': 'adminadmin',
                                               'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
                                               'spark.hadoop.fs.s3a.path.style.access': 'true',
                                               'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
                                               'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'})

build_orders_header_ft = SparkSubmitOperator(task_id='build_orders_header_ft',
                                             conn_id='spark-conn',
                                             application='/opt/bitnami/airflow/jobs/build_orders_header_ft.py',
                                             dag=dag,
                                             jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar',
                                             conf={'spark.hadoop.fs.s3a.access.key': 'admin',
                                                   'spark.hadoop.fs.s3a.secret.key': 'adminadmin',
                                                   'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
                                                   'spark.hadoop.fs.s3a.path.style.access': 'true',
                                                   'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
                                                   'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'})

build_customers_dim >> build_dates_dim >> build_sellers_dim >> build_products_dim >> build_orders_header_ft
