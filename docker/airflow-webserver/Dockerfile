FROM bitnami/airflow

USER root

RUN apt update && apt install -y default-jdk

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

RUN /opt/bitnami/airflow/venv/bin/pip install apache-airflow apache-airflow-providers-apache-spark pyspark

USER 1001