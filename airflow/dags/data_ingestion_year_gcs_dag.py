import os
import zipfile
import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'http://data.gdeltproject.org/events/' 
URL_TEMPLATE = URL_PREFIX + '{{ execution_date.strftime(\'%Y\') }}.zip'
ZIP_FILE_TEMPLATE = AIRFLOW_HOME + '/{{ execution_date.strftime(\'%Y\') }}.zip'
CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/{{ execution_date.strftime(\'%Y\') }}.csv'
PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/{{ execution_date.strftime(\'%Y\') }}.parquet'
PARQUET_FILE = 'gdelt_{{ execution_date.strftime(\'%Y\') }}.parquet'

def unzip_archive(zip_file):
    if not zip_file.endswith('.zip'):
        logging.error("Zip file expected")
        return
    z = zipfile.ZipFile(file=zip_file, mode='r')    
    z.extractall()

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    parse_options = pv.ParseOptions(delimiter='\t')
    table = pv.read_csv(src_file, parse_options=parse_options)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# Annual data started in 1979 and monthly in January 2006.

default_args = {
    "owner": "airflow",
    "start_date": datetime(1979,1,1),
    "end_date": datetime(2006,1,1),
    "depends_on_past": False,
    "retries": 2,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)

with DAG(
    dag_id="data_ingest_year_gdelt_gcs_dag",
    schedule_interval="0 0 1 1 *",
    max_active_runs=2,
    default_args=default_args,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f'curl -sSL {URL_TEMPLATE} > {ZIP_FILE_TEMPLATE}'
    )

    unzip_dataset_task = PythonOperator(
        task_id="unzip_dataset_task",
        python_callable=unzip_archive,
        op_kwargs={
            "zip_file": f"{ZIP_FILE_TEMPLATE}"
        }
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{CSV_FILE_TEMPLATE}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw_gdelt/{PARQUET_FILE}",
            "local_file": f"{PARQUET_FILE_TEMPLATE}",
        },
    )

    download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> local_to_gcs_task
