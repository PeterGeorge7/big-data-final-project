import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Configuration
NUM_RECORDS = 5500
CITY = "London"


def generate_traffic_data():

    traffic_id = np.arange(9001, 9001 + NUM_RECORDS)
    base_date = datetime(2024, 1, 1)
    dates = [base_date + timedelta(hours=i) for i in range(NUM_RECORDS)]
    areas = ["Camden", "Chelsea", "Islington", "Southwark", "Kensington"]
    data = {
        "traffic_id": traffic_id,
        "date_time": dates,
        "city": [CITY] * NUM_RECORDS,
        "area": np.random.choice(areas, size=NUM_RECORDS),
        "vehicle_count": np.random.randint(0, 5000, size=NUM_RECORDS),
        "avg_speed_kmh": np.random.uniform(3, 120, size=NUM_RECORDS),
        "accident_count": np.random.randint(0, 10, size=NUM_RECORDS),
        "congestion_level": np.random.choice(
            ["Low", "Medium", "High"], size=NUM_RECORDS
        ),
        "road_condition": np.random.choice(
            ["Dry", "Wet", "Snowy", "Damaged"], size=NUM_RECORDS
        ),
        "visibility_m": np.random.randint(50, 10000, size=NUM_RECORDS),
    }

    df = pd.DataFrame(data)

    df = pd.concat([df, df.sample(100)])

    df.reset_index(drop=True, inplace=True)

    df.loc[random.sample(range(len(df)), 50), "avg_speed_kmh"] = np.random.uniform(
        -50, -1, 50
    )

    df.loc[random.sample(range(len(df)), 30), "vehicle_count"] = np.random.randint(
        20000, 50000, 30
    )

    df.loc[random.sample(range(len(df)), 10), "accident_count"] = np.random.randint(
        50, 100, 10
    )

    df.loc[df.sample(frac=0.05).index, "area"] = np.nan

    df.loc[df.sample(frac=0.02).index, "traffic_id"] = np.nan

    df["date_time"] = df["date_time"].astype(object)
    df.loc[random.sample(range(len(df)), 20), "date_time"] = "2099-00-00 99:99"

    print("saving traffic_data.csv...")
    df.to_csv("../SyntheticData/traffic_data.csv", index=False)


if __name__ == "__main__":
    generate_traffic_data()
