# Deloitte Data Engineering Home Assignment

This repository contains an end-to-end data pipeline built on **Google Cloud Platform (GCP)** using **Apache Airflow**, **Dataform**, **Google Cloud Dataproc**, **PySpark**, **Google Cloud Pub/Sub**, and **Dataplex** to handle BigQuery transformations, real-time ingestion, data governance, and address data skewness.

---

## 🏗️ Architecture Overview

The pipeline orchestrates the following execution lifecycle:

1. **Real-Time Data Streaming (Pub/Sub Ingestion):**
   * Streams real-time event/mock data via a dedicated Pub/Sub publisher (`publish_to_pubsub.py`).
2. **Dataform Compilation & Invocation:** 
   * Compiles and executes Dataform models directly in **BigQuery** to perform initial SQL transformations on raw datasets.
3. **Ephemeral Dataproc Provisioning:** 
   * Spins up a single-node Dataproc cluster (`e2-standard-2`) configured for PySpark jobs.
4. **PySpark Execution & Skew Handling (`spark_skew_analysis.py`):** 
   * Reads target tables directly from BigQuery via the BigQuery Connector.
   * Performs high-cardinality aggregation (`groupBy("city")`) to handle data skew and compute city address totals.
5. **Data Governance & Metadata (Dataplex):**
   * Integrated **Dataplex Business Glossary** mapping business taxonomy and entity-level documentation across BigQuery columns.
6. **Automated Resource Cleanup:** 
   * Deletes the Dataproc cluster upon pipeline completion or failure (`trigger_rule="all_done"`) to eliminate idle compute costs.

---

## 🌟 Bonus Features Implemented

* **Real-time Pub/Sub Streaming:** Implemented `publish_to_pubsub.py` to handle asynchronous message publishing to GCP Pub/Sub topics.
* **Dataplex Governance:** Mapped BigQuery schema attributes to a centralized Dataplex Business Glossary for enhanced data lineage and metadata management.

---

## 📊 Pipeline & Spark Execution Proof

### Airflow DAG Success
![Airflow DAG Execution](composer_dag_complete.png)

### Spark UI & Skew Analysis
![Spark Execution Metrics](spark_dashboard.png)

---

## 📂 Repository Structure

* **`deloitte_pipeline_dag.py`** – Airflow DAG controlling the end-to-end workflow execution.
* **`spark_skew_analysis.py`** – PySpark job reading BigQuery tables, handling skewness, and performing aggregations.
* **`publish_to_pubsub.py`** – Script for publishing simulated streaming events to GCP Pub/Sub.
* **`generate_and_load_data.py`** – Script for generating initial test datasets and staging them into BigQuery.
* **`requirements.txt`** – Project dependencies for local testing and execution.
* **`composer_dag_complete.png`** – Proof of successful end-to-end DAG execution in Cloud Composer.
* **`spark_dashboard.png`** – Spark UI metrics and execution details.

---

## 🚀 Local Setup & Installation

1. Clone the repository:
   ```bash
   git clone [https://github.com/DavidBnm/deloitte-de-home-task.git](https://github.com/DavidBnm/deloitte-de-home-task.git)
   cd deloitte-de-home-task
