# Technical-Assesment
This is the Technical Assesment for the EMIS


This is the code to the techinical assesment given
-----------------------

The Task

An external system / supplier is sending patient data to our platform using the FHIR standard. Our analytics teams find this format difficult to work with when creating dashboards and visualizations. You are required to tranform these FHIR messages into a more workable format preferably in a tabular format. Include any documentation / commentary you deem necessary.


Analysis done :

PySpark and MongoDB are two different technologies that can be used for processing and storing nested JSON data, but they have different use cases and strengths.

PySpark is a distributed computing framework that is built on top of Apache Spark. It provides a powerful engine for processing large-scale data sets in parallel across a cluster of computers. PySpark provides a high-level API for working with structured and semi-structured data, including JSON. PySpark can efficiently handle nested JSON data and provides a rich set of operations for transforming and querying it.

MongoDB, on the other hand, is a NoSQL document database that is designed for storing and querying JSON-like documents. It provides a flexible schema-less data model that can handle nested JSON data easily. MongoDB also provides a powerful query language that supports rich data types, such as arrays and nested documents.

If your primary use case is to process and analyze large-scale data sets that are stored in nested JSON format, then PySpark may be a better choice. PySpark can handle data sets that are too large to fit into memory on a single machine, and it provides a wide range of tools for data transformation and analysis.

If your primary use case is to store and query nested JSON data, then MongoDB may be a better choice. MongoDB provides a flexible data model that can handle complex, nested data structures, and it provides a powerful query language for querying and manipulating this data.

Assumin the requirements as to process and analyze large-scale data sets, PySpark may be the best choice. The operational data store can be like in Presto/snowflake kind of dat store where it can provide interactive / in momory process engine and also has the capability to process analytics needs.

Saving the coplumnr file liek formats like parquet will better performances


The solution :

The soluttion is created  as dag in orchestartion tool Airflow. The dag in dags folder 'process_FHIR_data' process the data present in input folder where all the source data /staging data comes from the extermal resources. We can confiure event stream aslso as sync and can schedule the spark jo but the solution is considered normal batch ingestion.  But we can always combine stream and batch through spark providing ( Lambda pattern).

The input source data is input_jsons/data folder.

The dag is created with dynamic tasks based on the files with the concurrancy set to 10 at present which means can process 10 files at a time
but we can increase or decrease the concurrent execution of tasks based on infra available.

Each task is spark submit with all the input data files, scripts and packages which performs the json formats and
load it to either parquet file or Hive ( you can save to to any data base like Preso.

The task also performs certain data changes using the spark security module ..to mask/encrypt data related to personal health info, GDPR etc.

We can also implement secure coding practices by filtering out any null values etc ..

We can also implement auditing and monitoring by logging user actions to an audit log for monitoring purposes through access conttrol
using Delta Lake to store the data in a secure location.

we're limiting data exposure by only selecting and displaying necessary data in a public DataFrame

-----------------------------------------------------

Steps to run this code

1) Assuming an Airflow instance running on the machine ( with celery or sequential or any executor), Copy the src folder contents in the Airflow_home folder.
2) Create a varibale in airflow or set pyspark home( spark isntalltion path) ex: pyspark_app_home= '/Users/user.admin/airflow/dags'.

3) copy the data files in the input_jsons/data folder
4) create a folder as outputs for the destination folder for the processed data partioned by partion columns
5) Run the dag in Airflow







