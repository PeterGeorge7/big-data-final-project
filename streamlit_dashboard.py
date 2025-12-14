import streamlit as st
import pandas as pd
import io
from minio import Minio
from PIL import Image

# --- Configuration ---
# Set page layout to wide for better visualization
st.set_page_config(page_title="Urban Traffic Analytics", layout="wide")

MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"


# --- MinIO Connection Helper ---
@st.cache_resource
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )


def load_data(bucket, filename, file_type="csv"):
    client = get_minio_client()
    try:
        response = client.get_object(bucket, filename)
        data = response.read()
        response.close()
        response.release_conn()

        if file_type == "csv":
            return pd.read_csv(io.BytesIO(data))
        elif file_type == "parquet":
            return pd.read_parquet(io.BytesIO(data))
        elif file_type == "image":
            return Image.open(io.BytesIO(data))
    except Exception as e:
        st.error(f"Error loading {filename}: {e}")
        return None


# --- Main Dashboard App ---
def main():
    st.title("🚗 Urban Traffic Analytics Dashboard")
    st.markdown("### Weather Impact Analysis & Risk Prediction")

    # Create Tabs for the 3 main requirements
    tab1, tab2, tab3 = st.tabs(
        ["📊 Dataset Statistics", "🎲 Monte Carlo Simulation", "🔍 Factor Analysis"]
    )

    # --- TAB 1: Cleaned Dataset Statistics [cite: 186] ---
    with tab1:
        st.header("Cleaned Analytical Dataset (Silver Layer)")

        # Load merged data
        df = load_data("silver", "merged_analytical_data.parquet", "parquet")

        if df is not None:
            # Top-level metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Records", len(df))
            col2.metric(
                "Date Range",
                f"{df['date_time'].dt.date.min()} to {df['date_time'].dt.date.max()}",
            )
            col3.metric("Cities Covered", df["city"].nunique())

            # Show Sample Data
            st.subheader("Data Preview")
            st.dataframe(df.head())

            # Show Statistics
            st.subheader("Statistical Summary")
            st.write(df.describe())

            # Simple Visualization: Congestion Distribution
            st.subheader("Traffic Congestion Levels")
            congestion_counts = df["congestion_level"].value_counts()
            st.bar_chart(congestion_counts)

    # --- TAB 2: Monte Carlo Simulation [cite: 187] ---
    with tab2:
        st.header("Monte Carlo Risk Prediction (Gold Layer)")

        # Load Results CSV
        sim_df = load_data("gold", "simulation_results.csv", "csv")

        if sim_df is not None:
            # Calculate KPIs dynamically from the loaded results
            prob_jam = sim_df["is_traffic_jam"].mean() * 100
            prob_accident = sim_df["is_accident"].mean() * 100
            avg_risk = sim_df["risk_score"].mean()

            # Display KPIs
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Avg Risk Score (0-10)", f"{avg_risk:.2f}")
            kpi2.metric("Prob. of Traffic Jam", f"{prob_jam:.2f}%")
            kpi3.metric("Prob. of Accident", f"{prob_accident:.2f}%")

            st.divider()

            # Display the Generated Plot (Loading the PNG from Gold ensures exact match)
            st.subheader("Risk Distribution Plot")
            image = load_data("gold", "congestion_distribution.png", "image")
            if image:
                st.image(
                    image,
                    caption="Distribution of Simulated Risk Scores",
                    use_column_width=True,
                )

            # Raw Simulation Data
            with st.expander("View Raw Simulation Logs"):
                st.dataframe(sim_df)

    # --- TAB 3: Factor Analysis Insights [cite: 188] ---
    with tab3:
        st.header("Factor Analysis: Hidden Weather Drivers")

        st.markdown(
            """
        **Interpretation:**
        * **Factor 1:** Likely represents 'Weather Severity' (Rain, Wind, etc.)
        * **Factor 2:** Likely represents 'Traffic Flow' (Speed, Count)
        * **Factor 3:** Likely represents 'Accident Risk'
        """
        )

        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("Factor Loadings Table")
            loadings_df = load_data("gold", "factor_analysis_loadings.csv", "csv")
            if loadings_df is not None:
                st.dataframe(loadings_df)

        with col2:
            st.subheader("Correlation Heatmap")
            heatmap_img = load_data("gold", "factor_heatmap.png", "image")
            if heatmap_img:
                st.image(
                    heatmap_img,
                    caption="Correlation between Observed Variables and Hidden Factors",
                    use_column_width=True,
                )


if __name__ == "__main__":
    main()
