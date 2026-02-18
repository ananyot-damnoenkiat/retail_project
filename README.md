# Retail Project: Big Data ELT Pipeline with Spark & Airflow 🛒

![Python](https://img.shields.io/badge/Python-3.9-blue?style=flat&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4-orange?style=flat&logo=apachespark)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-red?style=flat&logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=flat&logo=docker)
![GCP](https://img.shields.io/badge/Google_Cloud-BigQuery-yellow?style=flat&logo=googlecloud)
![Terraform](https://img.shields.io/badge/Terraform-IaC-purple?style=flat&logo=terraform)

## 📖 Executive Summary
**Retail Project** is an end-to-end Data Engineering project designed to simulate a high-volume retail data environment. The goal is to ingest raw transaction data, validate its quality using **Apache Spark**, and transform it into an analytics-ready **Star Schema** in **Google BigQuery**.

This project demonstrates the ability to build **scalable data pipeline**, handle **data quality issues** (dirty data), and implement **modern data warehousing** practices-skills essential for ensuring data availability and integrity for analytics teams.

---

## 🏗 System Architecture
The pipeline follows a robust **ELT (Extract, Load, Transform)** pattern, orchestrated entirely by Airflow running in Docker containers.

![Project Workflow](./images/retail.drawio.svg)

---

## 🚀 Key Features & Technical Highlights
1. **Scalable Data Processing with PySpark**
- **Why:** Replaced traditional Pandas processing with PySpark to handle potential large-scale datasets (Big Data) efficiently.
- **Implementation:** Used Spark DataFrames to read CSVs, filter records, and write to Parquet format for optimized storage and query performance.

2. **Automated Data Quality Checks**
- **Why:** Toensure data integrity before it reaches the warehouse

- **Validation Logic:**
    - **Zero Tolerance for Negatives:** Automatically drops records with negative trransaction amounts.
    - **Null Checks:** Filter out transactions with missing critical information (e.g., Product Category).
    - **Logging:** Tracks and logs the number of dropped "bad records" for auditing.

3. **Data Modeling (Star Schema)**
- **Why:** To optimize the Data Warehouse for analytical queries.
- **Design:**
    - **Fact Table (fact_sales):** Contains quantitative data (Amount, Transaction Date).
    - **Dimension Table (dim_products):** Contains descriptive attributes (Category).
    - This normalization reduces data redundancy and improves query speed in BigQuery.

![Retail Schema](./images/retail-schema.svg)

4. **Infrastructure as Code (IaC)**
- **Why:** To ensure the cloud environment is reproducible and version-controlled.
- **Tool:** Terraform is used to provision GCS Buckets and BigQuery Datasets automatically.

---

## 🛠 Tech Stack
- **Cloud Provider:** Google Cloud Platform (GCP)
- **Infrastructure as Code:** Terraform
- **Containerization:** Docker & Docker Compose
- **Orchestration:** Apache Airflow
- **Processing:** PySpark
- **Cloud Storage:** Google Cloud Storage (GCS)
- **Data Warehouse:** Google BigQuery
- **Language:** Python, SQL

---

## ⚙️ How to Run
**Prerequisites**
- Docker Desktop (4GB+ RAM recommended)
- Google Cloud Platform Account (Service Account Key JSON)

**Step 1: Provision Infrastructure**
```bash
cd terraform
# Update project_id in main.tf
terraform init
terraform apply
```

**Step 2: Build & Start Containers**
Since we use a custom Dockerfile to inject Java/Spark dependencies:
```bash
# Place google_credentials.json in the root directory first
docker-compose build
docker-compose up -d
```

**Step 3: Configure Airflow**
1. Access Airflow UI at http://localhost:8080 (User/Pass: admin/admin)
2. Go to Admin > Connections.
3. Edit google_cloud_default:
- Project Id: Your GCP Project ID.
- Keyfile Path: /opt/airflow/google_credentials.json

**Step 4: Trigger the Pipeline**
Enable and trigger the retail_pipeline_v1 DAG. Watch the graph view as data flows from generation -> Spark processing -> GCS -> BigQuery.

---

## 📊 Data Flow Explanation
1. **generate_data:** Simulates a transactional system by creating a CSV file with 1000+ records, intentionally injecting 5% "dirty data" (negative amount, null category) to test the pipeline's robustness.
2. **spark_process:**
- Initializes a Spark Session.
- Reads the raw CSV.
- Validates data: Drops rows where amount < 0 or category is NULL.
- Writes the clean data to local storage in Parquet format (Columnar storage).
3. **upload_to_gcs:** Python operator uploads the Parquet files to the GCS Data Lake.
4. **load_to_bq:** Loads Parquet files into a staging_sales table in BigQuery.
5. **create_star_schema:** Executes SQL DDL/DML to transform the staging table into fact_sales and dim_products.

---

## 💡 Lessons Learned
- **Spark vs Pandas:** While **Pandas** is great for small data, implementing **PySpark** ensures the pipeline is future-proof and can scale to millions of rows without memory errors.
- **Parquet over CSV:** Swicthing to Parquet reduced file size and improved load times into BigQuery because it preserves schema information.
- **Importance of Data Quality:** Implementing validation at the Spark layer prevents "Garbage In, Garbage Out" scenarios in the dashboarding phase.

---

## 👤 Author
Ananyot Damnoenkiat

[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/ananyot-damnoenkiat)