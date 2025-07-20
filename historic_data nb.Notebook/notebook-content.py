# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3d1d5508-871a-406a-ab0d-d72e69a60f51",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "b1bc2e70-4b73-4f0d-b93c-d90884d68103",
# META       "known_lakehouses": [
# META         {
# META           "id": "3d1d5508-871a-406a-ab0d-d72e69a60f51"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d331beb7-3c11-aa12-415e-b81ff56b5f12",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
from datetime import date, timedelta
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType, TimestampType

def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

dates = [
    "2025-01-01", "2025-02-01", "2025-03-01", "2025-04-01", "2025-05-01", "2025-06-01", "2025-07-01", "2025-07-04"
]
schema = StructType([
                StructField("id", StringType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("elevation", DoubleType(), True),
                StructField("title", StringType(), True),
                StructField("magnitude", DoubleType(), True),
                StructField("place_description", StringType(), True),
                StructField("sig", LongType(), True),
                StructField("magType", StringType(), True),
                StructField("time", LongType(), True),
                StructField("updated", LongType(), True)
        ])
df = spark.createDataFrame([], schema=schema)

for i in range(7):

    j= i + 1
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={dates[i]}&endtime={dates[j]}"

    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json().get("features",[])

        if not data:
            print("No data received")
        else:
            json_data = json.dumps(data, indent=4)

        parsed_data = json.loads(json_data)

        flat_data = [
            {
                "id": item.get("id"),
                "longitude": float(item.get("geometry",{}).get("coordinates",[None,None,None])[0]),
                "latitude": float(item.get("geometry",{}).get("coordinates",[None,None,None])[1]),
                "elevation": float(item.get("geometry",{}).get("coordinates",[None,None,None])[2]),
                "title": item.get("properties",{}).get("title",None),
                "magnitude": safe_float(item.get("properties",{}).get("mag",0.0)),
                "place_description": item.get("properties",{}).get("place",None),
                "sig": item.get("properties",{}).get("sig",0),
                "magType": item.get("properties",{}).get("magType",None),
                "time": item.get("properties",{}).get("time",None),
                "updated": item.get("properties",{}).get("updated",None),
            }
            for item in parsed_data
        ]

        df2 = spark.createDataFrame(flat_data, schema=schema)
        df = df.union(df2)

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}") 



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col, to_timestamp, to_date, date_format, isnull, asc
    
# Handling nulls
df = df.withColumn("longitude", when(isnull(col("longitude")), 0).otherwise(col("longitude")))\
         .withColumn("latitude", when(isnull(col("latitude")), 0).otherwise(col("latitude")))\
         .withColumn("time", when(df.time.isNull(), 0).otherwise(df.time))

# Timestamp conversions
df = df.withColumn("time", ((df.time) / 1000).cast(TimestampType()))\
         .withColumn("updated", ((df.updated) / 1000).cast(TimestampType()))

# Date and time formatting
df = df.withColumn("event_date", to_date(to_timestamp(col("time"))))\
         .withColumn("event_time", date_format(to_timestamp(col("time")), "HH:mm:ss:SSS"))\
         .withColumn("updated_date", to_date(to_timestamp(col("updated"))))\
         .withColumn("updated_time", date_format(to_timestamp(col("updated")), "HH:mm:ss:SSS"))

# Removing old columns
df = df.drop("time", "updated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import reverse_geocoder as rg
import pycountry as pyc

from pyspark.sql.functions import when, col, udf
from datetime import date, timedelta

def get_location_details(lat, long):
    try:
        coordinates = (float(lat), float(long))
        result = rg.search(coordinates)[0]
        return (result.get('name'), result.get('cc'), result.get('admin1'))  
    except Exception as e:
        print(f"Error processing coordinates: {lat}, {long} -> {str(e)}")
        return (None, None, None)

def get_country_name(code):
    try:
        result = pyc.countries.get(alpha_2 = code.upper())
        return result.name if result else None
    except Exception as e:
        return None


# Define UDF return schema
result_schema = StructType([
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("State", StringType(), True)
])

location_udf = udf(get_location_details, result_schema)
country_name_udf = udf(get_country_name, StringType())

# Apply single UDF and extract both fields
df = (df
      .withColumn("location_data", location_udf(col("latitude"), col("longitude")))
      .withColumn("City", col("location_data.City"))
      .withColumn("Country_Code", col("location_data.Country"))
        .withColumn("State", col("location_data.State"))
      .drop("location_data"))
df = df.withColumn("Country", country_name_udf(col("Country_Code")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("append").saveAsTable("Gold_LH.Gold_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
