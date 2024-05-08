import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(dag_id='read_from_db', description='Read data from database using Spark',
          default_args={'owner': 'rami boukadida', 'start_date': airflow.utils.dates.days_ago(1)},
          schedule_interval='@daily')

start = PythonOperator(task_id='start', python_callable=lambda: print('Jobs started'), dag=dag)

spark_job = SparkSubmitOperator(task_id='spark_job', conn_id='spark-conn',
                                application='/opt/bitnami/airflow/jobs/read_data_from_db.py', dag=dag,
                                jars='/opt/bitnami/airflow/jars/mysql-connector-j-8.4.0.jar')

end = PythonOperator(task_id='end', python_callable=lambda: print('Jobs completed successfully'), dag=dag)

start >> end
