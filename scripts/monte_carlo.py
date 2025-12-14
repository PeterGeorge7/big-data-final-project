import pandas as pd
import numpy as np
import io
import matplotlib.pyplot as plt
from minio import Minio

# --- Configuration ---
MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"
SIMULATION_RUNS = 10000


def run_monte_carlo():
    print("--- Phase 5: Monte Carlo Simulation (Traffic Risk Prediction) ---")

    # 1. Connect to MinIO
    client = Minio(
        MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )

    # 2. Load Merged Analytical Dataset
    try:
        response = client.get_object("silver", "merged_analytical_data.parquet")
        data = io.BytesIO(response.read())
        df = pd.read_parquet(data)
        response.close()
        response.release_conn()
        print(f"Loaded dataset with {len(df)} records.")
    except Exception as e:
        print(f"Error loading merged data: {e}")
        return

    # --- FIX 1: Rename Column if needed ---
    if "visibility_m_x" in df.columns:
        df.rename(columns={"visibility_m_x": "visibility_m"}, inplace=True)

    # --- FIX 2: Ensure Numeric Data ---
    # Convert 'rain_mm' and 'wind_speed' to numeric, forcing errors to 0
    df["rain_mm"] = pd.to_numeric(df["rain_mm"], errors="coerce").fillna(0)
    df["wind_speed_kmh"] = pd.to_numeric(df["wind_speed_kmh"], errors="coerce").fillna(
        0
    )
    df["visibility_m"] = pd.to_numeric(df["visibility_m"], errors="coerce").fillna(
        10000
    )

    # 3. Define Base Risk from Congestion
    risk_mapping = {"Low": 1.0, "Medium": 3.0, "High": 5.0}
    df["Base_Risk"] = df["congestion_level"].map(risk_mapping).fillna(1.0)

    # [cite_start]4. Filter for "Bad Weather" Scenarios [cite: 144-148]
    def is_severe_weather(row):
        return (
            (row["rain_mm"] > 5.0)
            or (row["wind_speed_kmh"] > 40.0)
            or (row["visibility_m"] < 2000)
            or (row["temperature_c"] < 2)
        )

    severe_weather_df = df[df.apply(is_severe_weather, axis=1)].copy()

    if severe_weather_df.empty:
        print("Warning: No severe weather data found. Using full dataset.")
        severe_weather_df = df
    else:
        print(
            f"Simulation pool size (Severe Weather Only): {len(severe_weather_df)} records."
        )

    # 5. Run Monte Carlo Simulation
    print(f"Running {SIMULATION_RUNS} simulation runs...")

    simulation_results = []

    for i in range(SIMULATION_RUNS):
        # A. Sample one weather scenario
        scenario = severe_weather_df.sample(n=1, replace=True).iloc[0]

        # B. Calculate Continuous Risk Score (The "Different Bars" Logic)
        # Formula: Base Congestion + Rain penalty + Wind penalty + Visibility penalty
        # This adds variance so the bars aren't just 1, 3, or 5.

        weather_penalty = (scenario["rain_mm"] * 0.1) + (
            scenario["wind_speed_kmh"] * 0.05
        )

        if scenario["visibility_m"] < 1000:
            weather_penalty += 2.0

        # Add random noise (simulation uncertainty)
        noise = np.random.normal(0, 0.5)

        total_risk_score = scenario["Base_Risk"] + weather_penalty + noise

        # Clip score to be between 0 and 10
        total_risk_score = max(0, min(10, total_risk_score))

        # C. Probabilistic Accident Simulation (Fixing the "Zeros" issue)
        # Instead of just reading the old accident_count, we calculate PROBABILITY
        # Higher risk score = Higher chance of accident in this simulated hour

        accident_probability = total_risk_score / 20.0  # e.g., Score 8 = 40% chance
        is_accident = np.random.random() < accident_probability

        # D. Traffic Jam Logic
        is_jam = total_risk_score > 6.0  # Threshold for "Jam"

        simulation_results.append(
            {
                "run_id": i,
                "is_traffic_jam": is_jam,
                "is_accident": is_accident,
                "risk_score": total_risk_score,
            }
        )

    results_df = pd.DataFrame(simulation_results)

    # [cite_start]6. Calculate Probabilities [cite: 150-153]
    prob_jam = results_df["is_traffic_jam"].mean() * 100
    prob_accident = results_df["is_accident"].mean() * 100
    avg_risk = results_df["risk_score"].mean()

    print("\n--- Simulation Results ---")
    print(f"Average Risk Score (0-10): {avg_risk:.2f}")
    print(f"Probability of Traffic Jam: {prob_jam:.2f}%")
    print(f"Probability of Accident:    {prob_accident:.2f}%")

    # [cite_start]7. Save Deliverables to Gold [cite: 156-157]

    # A. CSV
    csv_buffer = io.BytesIO()
    results_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    client.put_object(
        "gold",
        "simulation_results.csv",
        csv_buffer,
        csv_buffer.getbuffer().nbytes,
        content_type="text/csv",
    )
    print(" -> Saved simulation_results.csv to Gold")

    # B. Plot (Now with a nice distribution)
    plt.figure(figsize=(10, 6))

    # Histogram with more bins for better detail
    plt.hist(
        results_df["risk_score"], bins=30, color="#ff9999", edgecolor="black", alpha=0.7
    )

    plt.axvline(
        avg_risk,
        color="red",
        linestyle="dashed",
        linewidth=1,
        label=f"Avg Risk: {avg_risk:.1f}",
    )
    plt.title("Distribution of Traffic Risk Scores (Monte Carlo Simulation)")
    plt.xlabel("Calculated Risk Score (0=Safe, 10=Extreme Danger)")
    plt.ylabel("Frequency (Simulated Runs)")
    plt.legend()
    plt.grid(axis="y", alpha=0.5)

    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format="png")
    img_buffer.seek(0)
    client.put_object(
        "gold",
        "congestion_distribution.png",
        img_buffer,
        img_buffer.getbuffer().nbytes,
        content_type="image/png",
    )
    print(" -> Saved congestion_distribution.png to Gold")


if __name__ == "__main__":
    run_monte_carlo()
