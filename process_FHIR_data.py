"""
 This is Airflow data ingestion dag which process the inout data from the input_jsons/data folder.
 The dag is created with dynamic tasks based on the files with the concurrancy set to 10 at present which means can process 10 files at a time
 but we can increase or decrease the concurrent execution of tasks based on infra available.

 Each task is spark submit with all the input data files, scripts and packages which performs the json formats and
 load it to either parquet file or Hive ( you can save to to any data base like Preso.

 The task also performs certain data changes using the spark security module ..to mask/encrypt data related to personal health info, GDPR etc.

  We can also implement secure coding practices by filtering out any null values etc ..

  We can also implement auditing and monitoring by logging user actions to an audit log for monitoring purposes through access conttrol
  using Delta Lake to store the data in a secure location.

  we're limiting data exposure by only selecting and displaying necessary data in a public DataFrame.

"""

from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
import os
local_tz = pendulum.timezone("Europe/London")


default_args = {
    'owner': 'vijaya_bhaskar',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 2, tzinfo=local_tz),
    'email': ['neo.vijayk@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='process_FHIR_data',
          default_args=default_args,
          catchup=False,
          #schedule_interval="0 * * * *",
          params={'project_source': './dags/spark',
                  'spark_submit': './dags/spark/Json_flatten_module.py'},
          concurrency=10,
          )

if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__

# TODO create a varibale in airflow or set pyspark home
#pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
pyspark_app_home= '/Users/user.admin/airflow/dags'


file_lst = os.listdir('./dags/spark/input_jsons/data/')

start = DummyOperator(task_id='data_sensor',dag=dag)

for file in file_lst :

    patient_name= file.split("_", 2)[0]+"_"+file.split("_", 2)[1]

    patient_data_ingestion = SparkSubmitOperator(task_id='data_ingestion_'+patient_name,
                                                  conn_id='spark_local',
                                                  application=f'{pyspark_app_home}/spark/Json_flatten_module.py',
                                                  application_args = [file, patient_name],
                                                  #total_executor_cores=4,
                                                  #packages="io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
                                                  executor_cores=2,
                                                  executor_memory='5g',
                                                  driver_memory='5g',
                                                  name='patient_data_ingestion',
                                                  num_executors = 1,
                                                  verbose=False,
                                                  execution_timeout=timedelta(minutes=10),
                                                  #py_files=['path/to/your/python/file.py'],
                                                  dag=dag
                                                  )

    start >> patient_data_ingestion
