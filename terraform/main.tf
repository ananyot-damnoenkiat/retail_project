provider "google" {
  credentials = file("../google_credentials.json")
  project     = "retail-project-ananyot"
  region      = "asia-southeast3"
}

# 1. Create GCS Bucket (Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = "retailscale-lake-ananyot"
  location      = "asia-southeast3"
  force_destroy = true
}

# 2. Create BigQuery Dataset (Data Warehouse)
resource "google_bigquery_dataset" "warehouse" {
  dataset_id = "retail_dw"
  location   = "asia-southeast3"
}