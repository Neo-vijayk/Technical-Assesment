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

Assumin the requirements as to process and analyze large-scale data sets, PySpark may be the best choice. The operational data store can be like in Presto kind of dat store where it can provide interactive / in momeory process engine and also has the capability to process analytics needs.




