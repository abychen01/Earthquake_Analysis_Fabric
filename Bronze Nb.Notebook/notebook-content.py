# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b6141be4-6ec1-4deb-9e3f-ee8c51de76bb",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "8be724d1-75c7-4a15-9b26-dc6747947d8b",
# META       "known_lakehouses": [
# META         {
# META           "id": "b6141be4-6ec1-4deb-9e3f-ee8c51de76bb"
# META         },
# META         {
# META           "id": "2f73d050-1f8b-4e9d-bd8e-6640ac335a1c"
# META         },
# META         {
# META           "id": "a389b12c-e773-475f-91eb-e98c19bc40da"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

tiers = ["Bronze","Silver","Gold"]
lh_paths = {}

for x in tiers:
    lh_paths[x] = f"{x}_LH."f"{x}_data"
    # container paths are stored in the dictionary
    print(lh_paths[x])

bronze_lh = lh_paths["Bronze"]
silver_lh = lh_paths["Silver"]
gold_lh = lh_paths["Gold"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
# for API requests
import json
# for converting json to python objects
from datetime import date, timedelta


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = date.today() - timedelta(1)
end_date = date.today()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
# assigned start and end dates to url for fetching yesterday's data

try:
    response = requests.get(url)
    # getting earthquake data for yesterday using API
    response.raise_for_status()
    # checking if response is successful

    data = response.json().get("features",[])
    # getting the features from the response in GeoJSON format

    if not data:
        print("No data received")
    else:
        json_data = json.dumps(data, indent=4)
        
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

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

parsed_data = json.loads(json_data)

flat_data = [
    {
        "id": item.get("id"),
        "longitude": float(item.get("geometry",{}).get("coordinates",[None,None,None])[0]),
        "latitude": float(item.get("geometry",{}).get("coordinates",[None,None,None])[1]),
        "elevation": float(item.get("geometry",{}).get("coordinates",[None,None,None])[2]),
        "title": item.get("properties",{}).get("title",None),
        "magnitude": float(item.get("properties",{}).get("mag",0.0)),
        "place_description": item.get("properties",{}).get("place",None),
        "sig": item.get("properties",{}).get("sig",None),
        "magType": item.get("properties",{}).get("magType",None),
        "time": item.get("properties",{}).get("time",None),
        "updated": item.get("properties",{}).get("updated",None),
    }
    for item in parsed_data
]

df = spark.createDataFrame(flat_data, schema=schema)
#display(df)
df.write.mode("overwrite").format("delta").saveAsTable(bronze_lh)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

output = {
    "bronze_lh": bronze_lh,
    "silver_lh": silver_lh,
    "gold_lh": gold_lh
}
mssparkutils.notebook.exit(json.dumps(output))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# output_data = {
#     "start_date": start_date.isoformat(),  # isoformat() converts date to string for sending the output data
#     "end_date": end_date.isoformat(),
#     "bronze_adls": file_path,
#     "silver_adls": silver_adls,
#     "gold_adls": gold_adls
# }
# 
# dbutils.jobs.taskValues.set(key="bronze_output", value=output_data)
