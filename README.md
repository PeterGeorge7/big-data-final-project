# 🚗 Weather Impact on Urban Traffic Analysis (Big Data Final Project)

This repository implements a modern predictive data lake system to analyze how weather conditions affect urban traffic patterns in London.

[cite_start]The system uses MinIO for tiered storage (Bronze/Silver/Gold) [cite: 7][cite_start], and HDFS for distributed processing [cite: 8][cite_start], with Python handling all ETL and analytical tasks (Monte Carlo Simulation [cite: 10] [cite_start]and Factor Analysis [cite: 11]).

## 📊 Data Lake Architecture

[cite_start]Our system follows a three-layer data lake architecture using MinIO, integrated with HDFS for processing [cite: 70-71].



| Layer | Storage | Data Format | Purpose | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | MinIO | Raw CSV | [cite_start]Securely stores raw, messy, and unsanitized data (with outliers, duplicates, and nulls) [cite: 22, 16-20]. | **Complete** |
| **Silver** | MinIO / HDFS | Cleaned Parquet | [cite_start]Stores cleaned, structured, and validated data, ready for analysis[cite: 76]. | **Next Step** |
| **Gold** | MinIO | Reports/Results | [cite_start]Stores final outputs, reports, and predictive models (e.g., simulation results)[cite: 79]. | Pending |

## 🛠️ Infrastructure Setup

[cite_start]The entire environment is containerized using Docker Compose[cite: 69].

| Service | Host Port | Purpose | Connection Type |
| :--- | :--- | :--- | :--- |
| **MinIO API** | `9000` | S3 API endpoint for all Python scripts. | TCP |
| **MinIO Console** | `9001` | Web UI for visual inspection of buckets. | HTTP |
| **HDFS NameNode** | `9870` | WebHDFS REST API and NameNode Web UI. | HTTP/REST |
| **HDFS RPC** | `9002` | Native HDFS communication (mapped internally to 9000). | RPC (Internal) |

### 1. Starting the Stack

1.  Ensure Docker is running.
2.  Navigate to the root directory of the repository.
3.  Start all services:
    ```bash
    docker-compose up -d
    ```
4.  Verify all containers are up: `docker ps`

### 2. Configuration Files

* **`docker-compose.yaml`**: Defines all services, ports, and volumes. **Crucially, the NameNode port has been re-mapped to `9002:9000` to avoid conflicts with MinIO's `9000:9000` port.**
* **`hadoop.env`**: Ensures HDFS replication is set to `1` (for our single-node DataNode) and defines the internal Namenode address (`hdfs://namenode:9000`) for container-to-container communication.

---

## 🚀 Project Phases & Execution

[cite_start]We are following the 7 project phases defined in the requirements[cite: 80].

### (DONE) Phase 1: Infrastructure & Data Ingestion (Bronze Layer)

* [cite_start]**Objective**: Set up MinIO buckets (`bronze`, `silver`, `gold`) [cite: 87-91] [cite_start]and upload raw CSVs [cite: 92-95].
* **Status**: Complete. The raw weather and traffic synthetic datasets are successfully stored in the MinIO `bronze` bucket.

| File | Location | Description |
| :--- | :--- | :--- |
| `weather_raw.csv` | `minio://bronze/` | [cite_start]Raw weather data (approx. 5000 records)[cite: 30]. |
| `traffic_raw.csv` | `minio://bronze/` | [cite_start]Raw traffic data (approx. 5000 records)[cite: 51]. |

***

### ➡️ NEXT STEP: Phase 2: Data Cleaning (Bronze → Silver)

This is the most critical ETL step, where messy data is transformed into a clean, analytical format.

* [cite_start]**Objective**: Transform messy raw data into clean analytical datasets[cite: 102].
* **Tasks**:
    1.  [cite_start]Remove duplicates[cite: 105].
    2.  [cite_start]Handle missing values[cite: 106].
    3.  [cite_start]Fix incorrect formats (dates, numbers, text)[cite: 107].
    4.  [cite_start]Remove or correcting outliers[cite: 108].
* **Action**: Write and run the Python script `scripts/etl_clean.py`. [cite_start]This script will save the cleaned data as **Parquet** files in the MinIO `silver` bucket [cite: 111-112].

### Future Steps

| Phase | Description | Key Deliverables (Saved to MinIO) |
| :--- | :--- | :--- |
| **Phase 3** | [cite_start]**HDFS Integration** (Silver → HDFS) [cite: 118-123] | [cite_start]Screenshot of uploaded Parquet files on HDFS[cite: 129]. |
| **Phase 4** | [cite_start]**Dataset Merging** (Create Analytical Base) [cite: 130-137] | [cite_start]Final merged dataset stored in Silver or Gold layer[cite: 139]. |
| **Phase 5** | [cite_start]**Monte Carlo Sim.** (Risk Prediction) [cite: 140-154] | [cite_start]`simulation_results.csv` and Congestion Plot (Gold) [cite: 156-157]. |
| **Phase 6** | [cite_start]**Factor Analysis** (Weather Drivers) [cite: 159-171] | [cite_start]Factor Loadings Table and Interpretation Report (Gold) [cite: 178-181]. |
| **Phase 7** | [cite_start]**Visualization** (Optional) [cite: 182] | [cite_start]Interactive Dashboard displaying results [cite: 185-188]. |
