import pandas as pd
import io
from minio import Minio

MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"


def merge_datasets():

    client = Minio(
        MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )

    def load_parquet(bucket, filename):
        try:
            print(f"Loading {filename}...")
            response = client.get_object(bucket, filename)
            data = io.BytesIO(response.read())
            response.close()
            response.release_conn()
            return pd.read_parquet(data)
        except Exception as e:
            print(f"Error loading {filename}: {e}")
            return None

    weather_df = load_parquet("silver", "weather_cleaned.parquet")
    traffic_df = load_parquet("silver", "traffic_cleaned.parquet")

    if weather_df is None or traffic_df is None:
        print("Failed to load datasets. Stopping.")
        return

    print(f"Loaded Weather Rows: {len(weather_df)}")
    print(f"Loaded Traffic Rows: {len(traffic_df)}")
    print("Merging datasets...")
    merged_df = pd.merge(weather_df, traffic_df, on=["date_time", "city"], how="inner")

    print(f"Merged Dataset Rows: {len(merged_df)}")

    output_buffer = io.BytesIO()
    merged_df.to_parquet(output_buffer, index=False)
    output_buffer.seek(0)

    file_name = "merged_analytical_data.parquet"

    client.put_object(
        "silver",
        file_name,
        data=output_buffer,
        length=output_buffer.getbuffer().nbytes,
        content_type="application/x-parquet",
    )
    print(f"Successfully saved {file_name} to MinIO Silver bucket.")


if __name__ == "__main__":
    merge_datasets()
