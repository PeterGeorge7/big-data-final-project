import pandas as pd
import numpy as np
import io
import matplotlib.pyplot as plt
import seaborn as sns
from minio import Minio
from factor_analyzer import FactorAnalyzer

# --- Configuration ---
MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"


def run_factor_analysis():
    print("--- Phase 6: Factor Analysis (Weather Impact Detection) ---")

    # 1. Connect to MinIO
    client = Minio(
        MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )

    # 2. Load Data from Silver
    try:
        response = client.get_object("silver", "merged_analytical_data.parquet")
        data = io.BytesIO(response.read())
        df = pd.read_parquet(data)
        print(f"Loaded {len(df)} records.")
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # 3. Prepare Data for Analysis [cite: 161-170]
    # We need only numeric columns
    # Fix potential column name issues from merge (x/y)
    if "visibility_m_x" in df.columns:
        df.rename(columns={"visibility_m_x": "visibility_m"}, inplace=True)

    # Select features for analysis
    features = [
        "temperature_c",
        "humidity",
        "rain_mm",
        "wind_speed_kmh",
        "visibility_m",  # Weather
        "vehicle_count",
        "avg_speed_kmh",
        "accident_count",  # Traffic
    ]

    # Ensure all are numeric and handle NaNs
    analysis_df = df[features].apply(pd.to_numeric, errors="coerce").fillna(0)

    # 4. Perform Factor Analysis
    print("Running Factor Analysis (3 Factors)...")
    fa = FactorAnalyzer(n_factors=3, rotation="varimax")
    fa.fit(analysis_df)

    # Get Factor Loadings (Correlations between variables and factors)
    loadings = pd.DataFrame(
        fa.loadings_,
        index=analysis_df.columns,
        columns=["Factor_1", "Factor_2", "Factor_3"],
    )

    print("\nFactor Loadings:")
    print(loadings)

    # 5. Save Deliverables to Gold [cite: 178-181]

    # A. Save Loadings Table (CSV)
    csv_buffer = io.BytesIO()
    loadings.to_csv(csv_buffer)
    csv_buffer.seek(0)
    client.put_object(
        "gold",
        "factor_analysis_loadings.csv",
        csv_buffer,
        csv_buffer.getbuffer().nbytes,
        content_type="text/csv",
    )
    print(" -> Saved factor_analysis_loadings.csv to Gold")

    # B. Generate Interpretation Heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(loadings, annot=True, cmap="coolwarm", center=0)
    plt.title("Factor Analysis: Weather Variables vs Traffic Patterns")
    plt.ylabel("Observed Variables")
    plt.xlabel("Latent Factors (Hidden Drivers)")

    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format="png")
    img_buffer.seek(0)
    client.put_object(
        "gold",
        "factor_heatmap.png",
        img_buffer,
        img_buffer.getbuffer().nbytes,
        content_type="image/png",
    )
    print(" -> Saved factor_heatmap.png to Gold")

    # C. Generate Simple Report
    report_text = f"""
    Factor Analysis Report
    ======================
    Dataset Rows: {len(df)}
    
    Interpretation Guidelines:
    - High positive values (>0.5) mean the variable strongly correlates with the factor.
    - Factor 1 likely represents 'Weather Severity' (Rain, Wind, Humidity).
    - Factor 2 likely represents 'Traffic Flow' (Speed, Vehicle Count).
    - Factor 3 likely represents 'Accident Risk'.
    
    See 'factor_heatmap.png' for visual details.
    """

    report_buffer = io.BytesIO(report_text.encode("utf-8"))
    client.put_object(
        "gold",
        "factor_analysis_report.txt",
        report_buffer,
        report_buffer.getbuffer().nbytes,
        content_type="text/plain",
    )
    print(" -> Saved factor_analysis_report.txt to Gold")


if __name__ == "__main__":
    run_factor_analysis()
