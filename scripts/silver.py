from minio import Minio
import pandas as pd
import numpy as np
from io import BytesIO

client = Minio("localhost:9000", "admin", "admin123", secure=False)


def clean_weather_data():

    try:
        obj = client.get_object("bronze", "weather_data.csv")
        weather_data = pd.read_csv(BytesIO(obj.read()))
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return

    print("Original Weather Data:")
    print(weather_data.head())

    print(weather_data.info())

    print(weather_data.describe())

    # drop duplicates
    weather_data.drop_duplicates(inplace=True)

    # fill na and check for data types
    weather_data = weather_data.dropna(subset=["weather_id"])
    weather_data["weather_id"] = weather_data["weather_id"].astype(int)

    weather_data["date_time"] = pd.to_datetime(
        weather_data["date_time"], errors="coerce"
    )
    weather_data = weather_data.dropna(subset=["date_time"])

    fill_columns = [
        "temperature_c",
        "humidity",
        "rain_mm",
        "wind_speed_kmh",
        "visibility_m",
        "air_pressure_hpa",
    ]

    for column in fill_columns:
        weather_data[column] = weather_data[column].fillna(
            weather_data[column].median()
        )

    weather_data["season"] = weather_data["season"].fillna(
        weather_data["season"].mode()[0]
    )
    weather_data["weather_condition"] = weather_data["weather_condition"].fillna(
        weather_data["weather_condition"].mode()[0]
    )

    weather_data["city"] = weather_data["city"].fillna("London")

    print("check Weather Data:")
    print(weather_data.head())

    print(weather_data.info())

    print(weather_data.describe())

    # drop outliers:
    weather_data["temperature_c"] = weather_data["temperature_c"].clip(
        lower=-20, upper=50
    )
    weather_data["humidity"] = weather_data["humidity"].clip(lower=-20, upper=120)
    weather_data["wind_speed_kmh"] = weather_data["wind_speed_kmh"].clip(upper=150)
    weather_data["visibility_m"] = weather_data["visibility_m"].clip(upper=10000)
    weather_data["rain_mm"] = weather_data["rain_mm"].clip(upper=50)

    print("check Weather Data:")
    print(weather_data.head())

    print(weather_data.info())

    print(weather_data.describe())

    parquet_buffer = BytesIO()
    weather_data.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    client.put_object(
        "silver",
        "weather_cleaned.parquet",
        length=parquet_buffer.getbuffer().nbytes,
        data=parquet_buffer,
        content_type="application/x-parquet",
    )


def clean_traffic():
    try:
        obj = client.get_object("bronze", "traffic_data.csv")
        traffic_data = pd.read_csv(obj)
    except Exception as e:
        print(f"Error reading traffic data: {e}")
        return
    print(" \n============================\n ")
    print(" ====== Original Traffic Data: =======")
    print(" \n============================ \n")

    print(traffic_data.head())
    print(traffic_data.info())
    print(traffic_data.describe())

    traffic_data.drop_duplicates(inplace=True)

    # fix nulls and check data types
    traffic_data = traffic_data.dropna(subset=["traffic_id"])
    traffic_data["traffic_id"] = traffic_data["traffic_id"].astype(int)

    traffic_data["date_time"] = pd.to_datetime(
        traffic_data["date_time"], errors="coerce"
    )
    traffic_data = traffic_data.dropna(subset=["date_time"])

    numeric_cols = ["vehicle_count", "avg_speed_kmh", "accident_count", "visibility_m"]
    for col in numeric_cols:
        traffic_data[col] = traffic_data[col].fillna(traffic_data[col].median())

    traffic_data["area"] = traffic_data["area"].fillna("Unknown")
    traffic_data["congestion_level"] = traffic_data["congestion_level"].fillna(
        traffic_data["congestion_level"].mode()[0]
    )

    # fix outliers and negative values
    traffic_data["avg_speed_kmh"] = traffic_data["avg_speed_kmh"].abs()

    traffic_data["vehicle_count"] = traffic_data["vehicle_count"].clip(upper=5000)

    traffic_data["accident_count"] = traffic_data["accident_count"].clip(upper=10)

    print("=============================\n ")
    print("check traffic data:")
    print(traffic_data.head())
    print(traffic_data.info())
    print(traffic_data.describe())

    # Save to Silver
    parquet_buffer = BytesIO()
    traffic_data.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    client.put_object(
        "silver",
        "traffic_cleaned.parquet",
        data=parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/x-parquet",
    )
    print("Saved traffic_cleaned.parquet to Silver bucket.")


def check_objects_in_silver():
    list_of_objs = client.list_objects("silver")
    objects = []
    for obj in list_of_objs:
        objects.append(obj.object_name)
    return objects


clean_weather_data()
clean_traffic()
print(check_objects_in_silver())
