from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
import sys
import shutil

def process_retail_data(input_file, output_dir):
    # 1. Start Spark Session (Local Mode)
    spark = SparkSession.builder \
        .appName("RetailDataCleaning") \
        .getOrCreate()
    
    print(f"Reading data from {input_file}")
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Data Validation & Cleansing
    print("Performing Data Quality Checks...")

    # Count total records
    total_records = df.count()

    # Filter Logic
    df_clean = df.filter(
        (col("amount") > 0) &
        (col("product_category").isNotNull())
    )

    clean_count = df_clean.count()
    print(f"Data Quality: Dropped {total_records - clean_count} bad records.")

    # Transformation
    # Add processing timestamp
    df_transformed = df_clean.withColumn("processed_at", current_timestamp())

    # Write to Parquet
    # Remove output directory if it exists
    try:
        shutil.rmtree(output_dir)
    except OSError:
        pass

    print(f"Writing validated data to {output_dir}")
    df_transformed.write.mode("overwrite").parquet(output_dir)

    spark.stop()

if __name__ == "__main__":
    process_retail_data(
        "/opt/airflow/data/raw_sales.csv",
        "/opt/airflow/data/processed_sales"
    )