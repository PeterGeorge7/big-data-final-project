# 🚗 Weather Impact on Urban Traffic Analysis (Big Data Final Project)

**Project Goal:** Design and implement a modern predictive data lake system to analyze how weather conditions (rain, visibility, wind) affect urban traffic patterns in London.

This system uses a hybrid architecture with **MinIO** (Object Storage) for data layering and **HDFS** (Distributed File System) for processing, with **Python** handling the ETL and advanced analytical pipelines (Monte Carlo Simulation & Factor Analysis).

---

## 🏗️ System Architecture

[cite_start]The project follows a "Bronze-Silver-Gold" data lake pattern integrated with HDFS [cite: 70-79].

| Layer      | Storage System | Format          | Description                                                                     |
| :--------- | :------------- | :-------------- | :------------------------------------------------------------------------------ |
| **Bronze** | MinIO          | `.csv` (Raw)    | Raw, messy ingestion layer containing duplicates, nulls, and outliers.          |
| **Silver** | MinIO & HDFS   | `.parquet`      | Cleaned, structured, and validated data. Synced to HDFS for distributed access. |
| **Gold**   | MinIO          | `.csv` / `.png` | Final analytical results, simulation reports, and visualization charts.         |

### Tech Stack

- **Infrastructure:** Docker & Docker Compose
- **Storage:** MinIO (S3 Compatible), Hadoop HDFS (NameNode/DataNode)
- **Language:** Python 3.9+
- **Libraries:** `pandas`, `minio`, `hdfs`, `numpy`, `matplotlib`, `seaborn`, `factor_analyzer`, `pyarrow`

---

## ⚙️ Setup & Installation (On-Premise)

Follow these steps to clone the repository and run the project locally on your machine.

### 1. Prerequisites

- **Docker Desktop** (running)
- **Python 3.8+** installed locally
- **Git**

### 2. Clone the Repository

```bash
git clone <YOUR_REPO_URL>
cd "Final Big Data Project"
3. Install Python Dependencies
Create a virtual environment (optional) and install the required libraries:

Bash

pip install pandas numpy minio hdfs matplotlib seaborn factor_analyzer pyarrow
4. Start the Infrastructure
We use Docker Compose to spin up MinIO, the HDFS Cluster, and the Auto-Bucket creator.

Important: The HDFS NameNode RPC port is mapped to 9002 on the host to avoid conflicts with MinIO.

Bash

docker-compose up -d
Verify the services are running:

MinIO Console: http://localhost:9001 (User: admin, Pass: admin123)

HDFS Web UI: http://localhost:9870

🚀 Usage Guide: Running the Pipeline
The project is executed in 6 Sequential Phases. Run these scripts in order from the root directory.

Phase 1: Data Ingestion (Bronze Layer)
Generates synthetic "messy" data and uploads it to the MinIO Bronze bucket .

Bash

# 1. Generate the raw data locally
python scripts/g_weather.py
python scripts/g_traffic.py

# 2. Upload to MinIO Bronze
python scripts/bronze.py
Phase 2: Data Cleaning (Bronze → Silver)
Reads raw CSVs, fixes outliers/nulls/formats, and saves as Parquet to MinIO Silver .

Bash

python scripts/etl_clean.py
Phase 3: HDFS Synchronization
Syncs the cleaned Parquet files from MinIO Silver to the HDFS cluster (/weather_data, /traffic_data) .

Note: Because of Docker networking (resolving internal container names), run this script using the Docker command below:

PowerShell

# Run this in PowerShell from the project root
docker run --rm --network finalbigdataproject_default `
  -v "${PWD}/scripts:/app" `
  -w /app `
  python:3.9-slim `
  /bin/bash -c "pip install minio hdfs && python hdfs_sync.py"
(If your network name is different, check docker network ls)

Phase 4: Dataset Merging
Joins the Weather and Traffic datasets on date_time and city into a single analytical dataset .

Bash

python scripts/merge_data.py
Phase 5: Monte Carlo Simulation (Risk Prediction)
Simulates 10,000 bad weather scenarios to predict traffic jam probabilities .

Output: simulation_results.csv and congestion_distribution.png in Gold Bucket.

Bash

python scripts/monte_carlo_sim.py
Phase 6: Factor Analysis
Identifies latent hidden drivers (e.g., "Weather Severity", "Traffic Stress") using statistical factor analysis .

Output: Heatmaps and Factor Loadings table in Gold Bucket.

Bash

python scripts/factor_analysis.py
📂 Project Structure
Plaintext

Final Big Data Project/
├── docker-compose.yaml      # Defines MinIO, HDFS, and Setup containers
├── hadoop.env               # HDFS Configuration (Replication=1)
├── SyntheticData/           # Local folder for generated raw CSVs
├── scripts/
│   ├── g_weather.py         # Generates messy weather data
│   ├── g_traffic.py         # Generates messy traffic data
│   ├── bronze.py            # Uploads raw data to Bronze
│   ├── etl_clean.py         # Cleaning logic (Pandas)
│   ├── hdfs_sync.py         # Syncs Silver layer to HDFS
│   ├── merge_data.py        # Merges datasets
│   ├── monte_carlo_sim.py   # Runs risk simulation
│   └── factor_analysis.py   # Runs statistical analysis
└── README.md                # Project documentation
🐛 Troubleshooting
1. Port Conflict Error (Bind for 0.0.0.0:9000 failed)

Cause: MinIO and HDFS both tried to use port 9000.

Solution: We mapped HDFS NameNode to 9002 externally in docker-compose.yaml. Do not change this back.

2. HDFS Connection Error (NameResolutionError)

Cause: Your PC cannot resolve the internal Docker container ID (e.g., c1cef07cc...).

Solution: Use the docker run command provided in Phase 3 to execute the HDFS sync script inside the Docker network.
```
