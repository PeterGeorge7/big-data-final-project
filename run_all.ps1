Write-Host "--- Starting Full Pipeline Run ---" -ForegroundColor Green

# 1. Generation
Write-Host "Step 1: Generating Data..."
python scripts/g_weather.py
python scripts/g_traffic.py

# 2. Ingestion
Write-Host "Step 2: Uploading to Bronze..."
python scripts/bronze.py

# 3. Cleaning
Write-Host "Step 3: Cleaning Data (Silver)..."
python scripts/silver.py

# 4. HDFS Sync
# Note: This might fail locally if not running inside Docker due to the hostname issue we fixed earlier.
# If running locally, ensure hdfs_sync.py uses 'localhost' instead of 'namenode'
Write-Host "Step 4: Syncing to HDFS..."
docker run --rm --network finalbigdataproject_default `
  -v "${PWD}/scripts:/app" `
  -w /app `
  python:3.9-slim `
  /bin/bash -c "pip install minio hdfs && python ./scripts/hdfs_sync.py"

# 5. Merging
Write-Host "Step 5: Merging Datasets..."
python scripts/merge_data.py

# 6. Analytics
Write-Host "Step 6: Running Analytics..."
python scripts/monte_carlo.py
python scripts/factor_analysis.py

Write-Host "--- Pipeline Complete! ---" -ForegroundColor Green