import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(dag_id='read_from_dw', description='Read data from data warehouse using Spark',
          default_args={'owner': 'rami boukadida', 'start_date': airflow.utils.dates.days_ago(1)},
          schedule_interval='@daily')

start = PythonOperator(task_id='start', python_callable=lambda: print('Jobs started'), dag=dag)

spark_job = SparkSubmitOperator(task_id='spark_job', conn_id='spark-conn',
                                application='/opt/bitnami/airflow/jobs/read_data_from_dw.py', dag=dag,
                                jars='/opt/bitnami/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/airflow/jars/hadoop-aws-3.3.4.jar',
                                conf={'spark.hadoop.fs.s3a.access.key': 'admin',
                                      'spark.hadoop.fs.s3a.secret.key': 'adminadmin',
                                      'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
                                      'spark.hadoop.fs.s3a.path.style.access': 'true',
                                      'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
                                      'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'})

end = PythonOperator(task_id='end', python_callable=lambda: print('Jobs completed successfully'), dag=dag)

start >> spark_job >> end
