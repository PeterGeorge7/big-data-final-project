import io
from minio import Minio
from hdfs import InsecureClient

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
HDFS_ENDPOINT = "http://namenode:9870"
HDFS_USER = "root"


def sync_silver_to_hdfs():
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    try:
        hdfs_client = InsecureClient(HDFS_ENDPOINT, user=HDFS_USER)
    except Exception as e:
        print(
            f"Failed to connect to HDFS at {HDFS_ENDPOINT}. Is the container running? Error: {e}"
        )
        return

    files_map = {
        "weather_cleaned.parquet": "/weather_data",
        "traffic_cleaned.parquet": "/traffic_data",
    }

    for file_name, hdfs_folder in files_map.items():
        try:
            response = minio_client.get_object("silver", file_name)
            data_bytes = response.read()
            response.close()
            response.release_conn()
        except Exception as e:
            print(f" -> Error reading from MinIO: {e}")
            continue

        target_path = f"{hdfs_folder}/{file_name}"

        try:
            if not hdfs_client.content(hdfs_folder, strict=False):
                hdfs_client.makedirs(hdfs_folder)
                print(f" -> Created HDFS directory: {hdfs_folder}")

            with hdfs_client.write(target_path, overwrite=True) as writer:
                writer.write(data_bytes)
            print(f" -> Successfully uploaded to HDFS: {target_path}")

        except Exception as e:
            print(f" -> Error writing to HDFS: {e}")

    print("\n--- HDFS Sync Completed ---")


if __name__ == "__main__":
    sync_silver_to_hdfs()
