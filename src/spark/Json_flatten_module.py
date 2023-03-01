"""""

Generic function to flatten any Json with any levels of nesting, which can save a lot of time.

It uses Spark provideed  functions to handle Jsonslike  Explode and Explode_outer functions.
The program marks each level of json with *1, *2 like that.. and “->” shows the child node of a parent node.

"""""



from typing import Final, Dict, Tuple
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame as SDF
from pyspark.sql.functions import  date_format, lit
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.functions import col, sha2
import logging
import os, sys
import argparse
from pyspark.sql.types import StructType, ArrayType

def rename_dataframe_cols(df: SDF, col_names: Dict[str, str]) -> SDF:
    """""
    dataframe changes 
    
    """""

    return df.select(*[col(col_name).alias (col_names.get(col_name, col_name)) for col_name in df.columns])

def update_column_names(df: SDF, index: int) -> SDF:
    df_temp = df
    all_cols = df_temp.columns
    new_cols = dict((column, f"{column}*{index}") for column in all_cols)
    df_temp=df_temp.transform(lambda df_x: rename_dataframe_cols (df_x, new_cols))

    return df_temp


def flatten_json(df_arg: SDF, index: int = 1) -> SDF:
    """
    Flatten Json in a spark dataframe using recursion approach.

    """

    # Update all column names with index 1 first

    df = update_column_names(df_arg, index) if index == 1 else df_arg

    # Get all field names in the dataframe

    fields = df.schema.fields

    # For all columns in the dataframe  do this
    for field in fields:

        data_type = str(field.dataType)
        column_name = field.name
        first_10_chars = data_type [0:10]
        # If it is an Array column
        if first_10_chars == 'ArrayType(':
            # Explode Array column
            df_temp = df.withColumn(column_name,explode_outer(col(column_name)))
            return flatten_json(df_temp, index + 1)
        # If it is a json object
        elif first_10_chars == 'StructType':
            current_col = column_name
            append_str = current_col
            # Get data type of current column
            data_type_str = str(df.schema [current_col].dataType)
            # Change the column name if the current column name exists in the data type string
            df_temp = df.withColumnRenamed(column_name, column_name + "#1") \
                if column_name in data_type_str else df
            current_col = current_col + "#1" if column_name in data_type_str else current_col
            # Expand struct column values
            df_before_expanding = df_temp.select(f"{current_col}.*")
            newly_gen_cols = df_before_expanding.columns
            # Find next level value for the column
            begin_index =  append_str.rfind('*')
            end_index = len(append_str)

            level = append_str[begin_index + 1: end_index]
            next_level = int (level) + 1
            # Update column names with new level
            custom_cols = dict((field, f"{append_str}->{field}*{next_level}") for field in newly_gen_cols)
            df_temp2 = df_temp.select("*", f"{current_col}.*").drop(current_col)
            df_temp3 = df_temp2.transform(lambda df_x: rename_dataframe_cols (df_x, custom_cols))
            return flatten_json(df_temp3, index + 1)
    return df



if __name__== "__main__" :
    spark = SparkSession.builder.\
        appName("flatten_the_patients_json_data").\
        master("local[*]").\
        getOrCreate()

    if sys.argv[1] is not None:
        print("The file name to be processsed ", sys.argv[1])
    if sys.argv[2] is not None:
        table_name = sys.argv[2]

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    sc= spark.sparkContext
    df2 = spark.read.option("multiline", "true").json("/Users/user.admin/airflow/dags/spark/input_jsons/data/"+sys.argv[1], mode="FAILFAST")
    df2.show(5, False)
    df2.printSchema()

    df3= flatten_json(df2)
    df3.printSchema()
    print((df3.count(), len(df3.columns)))
    date_cols = [c.name for c in df3.schema.fields if ( 'date' in c.name or 'Date' in c.name )] #or 'Datetime' in c.name
    for col_name in date_cols:
        df3 = df3.withColumn(col_name, to_date(df3[col_name], 'yyyy-MM-dd'))

    datetime_cols = [c.name for c in df3.schema.fields if ('Datetime' in c.name or 'datetime' in c.name)]  # or 'Datetime' in c.name
    for col_name in date_cols:

        df3= df3.withColumn("datetime_ts", to_timestamp(df3[col_name], "yyyy-MM-dd'T'HH:mm:ss"))

    #data_checks(df)
    #df3 = df3.withColumn("masked_date_of_birth", date_format(df3["entry*1->resource*2->birthDate*3"], lit("xxxx-xx-xx")))
    #df3 = df3.withColumn("entry*1->resource*2->birthDate*3",
     #                    functions.mask(col("entry*1->resource*2->birthDate*3"), "X", 10))
    # Encrypt sensitive data using AES-256
    #df3 = df3.withColumn("entry*1->resource*2->birthDate*3", functions.encrypt(col("entry*1->resource*2->birthDate*3"), "password", "AES-256"))

    # encrypt or Mask sensitive data like name or birth date
    df3 = df3.withColumn("entry*1->resource*2->birthDate*3", expr("base64(aes_encrypt('entry*1->resource*2->birthDate*3', '1234567890abcdef', 'ECB', 'PKCS'))"))

    df3.printSchema()
    df3.show(10, False)
    df3.write.format("parquet").partitionBy("entry*1->resource*2->address*3->country*4", "entry*1->resource*2->address*3->state*4","entry*1->resource*2->id*3").mode("overwrite").save("/Users/user.admin/airflow/dags/spark/outputs/refined_patient_data.parquet")

    # Stop the Spark session

    spark.stop()

