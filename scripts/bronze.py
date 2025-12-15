from io import BytesIO
import pandas as pd
from numpy import random
from minio import Minio


def upload_to_bronze():
    client = Minio("localhost:9000", "admin", "admin123", secure=False)

    client.fput_object(
        "bronze",
        "weather_data.csv",
        "D:\\Data Engineer\\Data Engineering Projects\\Final Big Data Project\\SyntheticData\\weather_data.csv",
    )
    client.fput_object(
        "bronze",
        "traffic_data.csv",
        "D:\\Data Engineer\\Data Engineering Projects\\Final Big Data Project\\SyntheticData\\traffic_data.csv",
    )

    list_of_objs = client.list_objects("bronze")
    objects = []
    for obj in list_of_objs:
        objects.append(obj.object_name)
    return objects


print(upload_to_bronze())
