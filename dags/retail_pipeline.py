from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
import os
import glob
from datetime import datetime, timedelta

# Config
PROJECT_ID = "retail-project-ananyot"
BUCKET_NAME = "retailscale-lake-ananyot"
DATASET_NAME = "retail_dw"

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

def _upload_folder_to_gcs(local_folder, bucket_name, gcs_prefix):
    # Upload all files in a local folder to GCS
    client = storage.Client()
    bucket_name = client.bucket(bucket_name)
    files = glob.glob(f"{local_folder}/*.parquet")

    for file_path in files:
        file_name = os.path.basename(file_path)
        blob = bucket_name.blob(f"{gcs_prefix}/{file_name}")
        blob.upload_from_filename(file_path)
        print(f"Uploaded {file_name}")

with DAG(
    'retail_pipeline_v1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Task 1: Generate Data (Simulated by a Python function)
    t1_gen_data = BashOperator(
        task_id='generate_data',
        bash_command='python /opt/airflow/scripts/generate_data.py'
    )

    # Task 2: Process Data with Spark (Data Validation & Cleansing)
    t2_spark_process = BashOperator(
        task_id='spark_process',
        bash_command='python /opt/airflow/scripts/spark_process.py'
    )

    # Task 3: Upload Parquet to GCS
    t3_upload_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=_upload_folder_to_gcs,
        op_kwargs={
            'local_folder': '/opt/airflow/data/processed_sales',
            'bucket_name': BUCKET_NAME,
            'gcs_prefix': 'processed_data'
        }
    )

    # Task 4: Load to BigQuery (Staging Table)
    t4_load_bq = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket=BUCKET_NAME,
        source_objects=['processed_data/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.staging_sales",
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    # Task 5: Data Modeling (Star Schema) - Create Fact/Dim Table
    create_star_schema_sql = f"""
    -- Create Dimension: Products
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.dim_products` AS
    SELECT DISTINCT product_category
    FROM `{PROJECT_ID}.{DATASET_NAME}.staging_sales`;

    -- Create Fact: Sales (Link to Dimension)
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.fact_sales` AS
    SELECT
        transaction_id,
        customer_id,
        amount,
        transaction_date,
        processed_at,
        product_category
    FROM `{PROJECT_ID}.{DATASET_NAME}.staging_sales`; 
    """

    t5_transform_dw = BigQueryInsertJobOperator(
        task_id='create_star_schema',
        configuration={
            "query": {
                "query": create_star_schema_sql,
                "useLegacySql": False
            }
        }
    )

    t1_gen_data >> t2_spark_process >> t3_upload_gcs >> t4_load_bq >> t5_transform_dw