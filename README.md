# olist-sales-ft

TODO : setup data warehouse with airflow each time the pipeline is run

## Project Description

...

## Project Structure

```
olist-sales-ft
│   dags/
│   docker/
│   jars/
│   jobs/
│   logs/
│   .gitignore
│   compose.yaml
│   data-warehouse.sql
│   README.md
```

## Project Setup

### 1) Build and run compose file

```bash
docker-compose up
```

### 2) Setup data warehouse

run ```data-warehouse.sql``` script in mysql container

### 3) Setup spark connection in [airflow](http://localhost:8080/home)

follow
this [guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui)

<table>
  <tr>
    <th>Airflow login</th>
    <td>admin</td>
  </tr>
  <tr>
    <th>Airflow password</th>
    <td>admin</td>
  </tr>
  <tr>
    <th>Connection Id</th>
    <td>spark-conn</td>
  </tr>
  <tr>
    <th>Connection Type</th>
    <td>spark</td>
  </tr>
  <tr>
    <th>Host</th>
    <td>spark://spark</td>
  </tr>
  <tr>
    <th>Port</th>
    <td>7077</td>
  </tr>
</table>

### 3) Upload [dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) to datalake

use [minio web interface](http://localhost:9000) to upload dataset into olist bucket
<table>
  <tr>
    <th>login</th>
    <td>admin</td>
  </tr>
  <tr>
    <th>password</th>
    <td>adminadmin</td>
  </tr>
</table>

### 4) Download jars

download the following jars and place them in the jars folder

[mysql-connector-j](https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar) >>
used to connect to data warehouse

[hadoop-aws](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar) & [aws-java-sdk](https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar) >>
used to connect to datalake
