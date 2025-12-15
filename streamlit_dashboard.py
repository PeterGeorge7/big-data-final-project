import streamlit as st
import pandas as pd
import io
from minio import Minio
from PIL import Image

# --- Configuration ---
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

    # 1. LOAD DATA FIRST
    df = load_data("silver", "merged_analytical_data.parquet", "parquet")

    if df is None:
        st.error("Failed to load data from Silver Layer. Please check MinIO.")
        return

    # Ensure date_time is actually a datetime object (Crucial for filtering)
    if not pd.api.types.is_datetime64_any_dtype(df["date_time"]):
        df["date_time"] = pd.to_datetime(df["date_time"])

    # 2. SIDEBAR FILTERS
    st.sidebar.header("Filter Options")

    # --- A. City Filter ---
    city_list = ["All"] + list(df["city"].unique())
    selected_city = st.sidebar.selectbox("Select City", city_list)

    # --- B. Date Range Filter ---
    min_date = df["date_time"].min().date()
    max_date = df["date_time"].max().date()

    st.sidebar.subheader("Select Date Range")
    try:
        start_date, end_date = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),  # Default to full range
            min_value=min_date,
            max_value=max_date,
        )
    except ValueError:
        st.sidebar.error("Please select a valid Start and End date.")
        start_date, end_date = min_date, max_date

    # 3. APPLY FILTERS TO DATA
    # Filter by City
    if selected_city != "All":
        df = df[df["city"] == selected_city]

    # Filter by Date
    mask = (df["date_time"].dt.date >= start_date) & (
        df["date_time"].dt.date <= end_date
    )
    df = df[mask]

    # Show User what is selected
    st.sidebar.success(f"Records Found: {len(df)}")

    st.markdown("### Weather Impact Analysis & Risk Prediction")

    # 4. Create Tabs
    tab1, tab2, tab3 = st.tabs(
        ["📊 Dataset Statistics", "🎲 Monte Carlo Simulation", "🔍 Factor Analysis"]
    )

    # --- TAB 1: Cleaned Dataset Statistics ---
    with tab1:
        st.header("Cleaned Analytical Dataset (Silver Layer)")

        # Top-level metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Records", len(df))
        if not df.empty:
            col2.metric("Date Range", f"{start_date} to {end_date}")
            col3.metric("Cities Covered", df["city"].nunique())

        # Show Sample Data
        st.subheader("Data Preview")
        st.dataframe(df.head())

        # Show Statistics
        st.subheader("Statistical Summary")
        st.write(df.describe())

        # Simple Visualization: Congestion Distribution
        st.subheader("Traffic Congestion Levels")
        if not df.empty:
            congestion_counts = df["congestion_level"].value_counts()
            st.bar_chart(congestion_counts)
        else:
            st.warning("No data matches your filters.")

    # --- TAB 2: Monte Carlo Simulation ---
    with tab2:
        st.header("Monte Carlo Risk Prediction (Gold Layer)")
        st.info(
            "Note: These results are based on the full simulation run (Gold Layer) and are not affected by the sidebar filters."
        )

        # Load Results CSV
        sim_df = load_data("gold", "simulation_results.csv", "csv")

        if sim_df is not None:
            # Calculate KPIs dynamically
            prob_jam = sim_df["is_traffic_jam"].mean() * 100
            prob_accident = sim_df["is_accident"].mean() * 100
            avg_risk = sim_df["risk_score"].mean()

            # Display KPIs
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Avg Risk Score (0-10)", f"{avg_risk:.2f}")
            kpi2.metric("Prob. of Traffic Jam", f"{prob_jam:.2f}%")
            kpi3.metric("Prob. of Accident", f"{prob_accident:.2f}%")

            st.divider()

            # Display the Generated Plot
            st.subheader("Risk Distribution Plot")
            image = load_data("gold", "congestion_distribution.png", "image")
            if image:
                st.image(
                    image,
                    caption="Distribution of Simulated Risk Scores",
                    use_container_width=True,
                )

            # Raw Simulation Data
            with st.expander("View Raw Simulation Logs"):
                st.dataframe(sim_df)

    # --- TAB 3: Factor Analysis Insights ---
    with tab3:
        st.header("Factor Analysis: Hidden Weather Drivers")
        st.info("Note: Factor Analysis is a static model built on the entire dataset.")

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
                    use_container_width=True,
                )


if __name__ == "__main__":
    main()
