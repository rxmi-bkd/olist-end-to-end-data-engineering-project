# olist-end-to-end-data-engineering-project

## Description

### Infrastructure

<img src="images/olist-architecture.drawio.svg">

### Pipeline

ETL stands for Extract, Transform, Load. An ETL pipeline is a set of processes used to collect data from various
sources, transform it into a format that is suitable for analysis or storage, and then load it into a target
destination, such as a database, data warehouse, or data lake.

In this project, we focusing on generating an orders fact table from the dataset provided
by [Olist](https://www.kaggle.com/olistbr/brazilian-ecommerce) in order to analyze the sales performance of the company.

<table>
    <tr>
        <th>Component</th>
        <th>Usage</th>
    </tr>
    <tr>
        <td>Minio</td>
        <td>Minio serves as the data lake where raw data is stored before being processed.</td>
    </tr>
    <tr>
        <td>Airflow</td>
        <td>Airflow is used to orchestrate the ETL pipeline.</td>
    </tr>
    <tr>
        <td>Spark & Python</td>
        <td>PySpark is used to process the raw data and generate the orders fact table.</td>
    </tr>
    <tr>
        <td>MySQL</td>
        <td>MySQL is used as the data warehouse where the orders fact table is stored.</td>
    </tr>
</table>

### Orders Fact Table

<img src="images/olist_dw.png">

With these tables, we can analyze the sales performance of the company. For example, we can calculate the average order
value.

```sql
select year(order_date)   as year,
       month(order_date)  as month,
       avg(price)         as avg_price,
       avg(freight_value) as avg_freight_value,
       avg(price + freight_value) as avg_total_value
from orders_fact
group by year, month
order by year, month;

+------+-------+------------------+-------------------+------------------+
| year | month | avg_total_price  | avg_total_freight | avg_total_order  |
+------+-------+------------------+-------------------+------------------+
| 2016 | 9     | 44.56            | 14.57             | 59.13            |
| 2016 | 10    | 136.38           | 20.11             | 156.50           |
| 2016 | 12    | 10.9             | 8.72              | 19.62            |
| 2017 | 1     | 125.98           | 17.67             | 143.65           |
| 2017 | 2     | 126.76           | 19.98             | 146.74           |
| 2017 | 3     | 124.78           | 19.23             | 144.02           |
| 2017 | 4     | 134.10           | 19.56             | 153.66           |
| 2017 | 5     | 122.36           | 19.37             | 141.73           |
| 2017 | 6     | 120.86           | 19.52             | 140.37           |
| 2017 | 7     | 110.21           | 19.24             | 129.45           |
| 2017 | 8     | 116.90           | 19.19             | 136.09           |
| 2017 | 9     | 129.25           | 19.87             | 149.12           |
| 2017 | 10    | 124.81           | 19.75             | 144.55           |
| 2017 | 11    | 116.59           | 19.49             | 136.08           |
| 2017 | 12    | 117.93           | 18.97             | 136.90           |
| 2018 | 1     | 115.74           | 19.16             | 134.91           |
| 2018 | 2     | 110.03           | 18.60             | 128.64           |
| 2018 | 3     | 119.66           | 20.92             | 140.58           |
| 2018 | 4     | 124.97           | 20.45             | 145.42           |
| 2018 | 5     | 125.74           | 19.34             | 145.08           |
| 2018 | 6     | 122.23           | 22.26             | 144.49           |
| 2018 | 7     | 126.27           | 23.01             | 149.28           |
| 2018 | 8     | 117.92           | 20.51             | 138.43           |
| 2018 | 9     | 145.00           | 21.46             | 166.46           |
+------+-------+------------------+-------------------+------------------+
```

A lot of other questions can be answered using this dataset. But we need to build other facts tables to answer them.
By exploring these questions, you can gain valuable insights into customer behavior, operational efficiency, product
performance, and overall business health. This can help in making data-driven decisions to improve various aspects of
the e-commerce operation.

## Setup

### 1) Setup docker

```bash
docker-compose up
```

### 2) Setup Spark and MySQL connection in [airflow](http://localhost:8080/home)

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
</table>

<table>
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

<table>
    <tr>
        <th>Connection Id</th>
        <td>mysql-conn</td>
    </tr>
    <tr>
        <th>Connection Type</th>
        <td>mysql</td>
    </tr>
    <tr>
        <th>MySQL login</th>
        <td>admin</td>
    </tr>
    <tr>
        <th>MySQL password</th>
        <td>admin</td>
    </tr>
    <tr>
        <th>Host</th>
        <td>database</td>
    </tr>
    <tr>
        <th>Port</th>
        <td>3306</td>
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

## Possible Improvements

For the moment, our orders fact table pipeline is designed to recreate tables every time the pipeline is run. This is not ideal for a production environment, where we would want to update the tables incrementally. 