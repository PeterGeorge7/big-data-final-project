import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

NUM_RECORDS = 5500
CITY = "London"


def generate_weather_data():
    weather_id = np.arange(5001, 5001 + NUM_RECORDS)
    base_date = datetime(2024, 1, 1)

    dates = [base_date + timedelta(hours=i) for i in range(NUM_RECORDS)]

    data = {
        "weather_id": weather_id,
        "date_time": dates,
        "city": [CITY] * NUM_RECORDS,
        "season": [],
        "temperature_c": [],
        "humidity": np.random.randint(20, 100, size=NUM_RECORDS),
        "rain_mm": np.random.exponential(scale=5, size=NUM_RECORDS),
        "wind_speed_kmh": np.random.uniform(0, 80, size=NUM_RECORDS),
        "visibility_m": np.random.randint(50, 10000, size=NUM_RECORDS),
        "weather_condition": np.random.choice(
            ["Clear", "Rain", "Fog", "Storm", "Snow"], size=NUM_RECORDS
        ),
        "air_pressure_hpa": np.random.uniform(950, 1050, size=NUM_RECORDS),
    }

    # season and temperature based on month
    for d in dates:
        month = d.month
        if month in [12, 1, 2]:
            data["season"].append("Winter")
            temp = np.random.uniform(-5, 15)
        elif month in [3, 4, 5]:
            data["season"].append("Spring")
            temp = np.random.uniform(8, 15)
        elif month in [6, 7, 8]:
            data["season"].append("Summer")
            temp = np.random.uniform(10, 35)
        elif month in [9, 10, 11]:
            data["season"].append("Autumn")
            temp = np.random.uniform(8, 15)
        data["temperature_c"].append(temp)

    df = pd.DataFrame(data)

    df = pd.concat([df, df.sample(100)])

    df.reset_index(drop=True, inplace=True)

    # outliers
    df.loc[random.sample(range(len(df)), 50), "temperature_c"] = np.random.choice(
        [-30, 60], 50
    )
    df.loc[random.sample(range(len(df)), 50), "humidity"] = np.random.choice(
        [-10, 150], 50
    )
    df.loc[random.sample(range(len(df)), 50), "rain_mm"] = np.random.uniform(
        80, 150, 50
    )
    df.loc[random.sample(range(len(df)), 50), "wind_speed_kmh"] = np.random.uniform(
        150, 250, 50
    )
    df.loc[random.sample(range(len(df)), 50), "visibility_m"] = np.random.randint(
        20000, 50000, 50
    )
    # Null Values
    for col in df.columns:
        df.loc[df.sample(frac=0.05).index, col] = np.nan

    # Bad Date Formats
    df["date_time"] = df["date_time"].astype(object)
    df.loc[random.sample(range(len(df)), 20), "date_time"] = "2099-13-40 25:61"

    print("save weather_data.csv...")
    df.to_csv("../SyntheticData/weather_data.csv", index=False)


if __name__ == "__main__":
    generate_weather_data()
