# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b76bd102-ec40-46f9-84b5-f67911b55ea0",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "15439e8f-1bb8-436c-87c7-28e3ee464f69",
# META       "known_lakehouses": [
# META         {
# META           "id": "b76bd102-ec40-46f9-84b5-f67911b55ea0"
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

import pyodbc, os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

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

#temp...

df_creds = spark.read.parquet('Files/creds')

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]


vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

reddit_id = client.get_secret("redditID").value
reddit_secret = client.get_secret("redditSecret").value
reddit_user_agent = client.get_secret("redditUserAgent").value
server_password = client.get_secret("sql-server-password").value


conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=earthquake_analysis ;"
            f"UID=admin2;"
            f"PWD={server_password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#testing...

with pyodbc.connect(conn_str, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            select 
                top 1 event_date 
            from 
                [dbo].[gold_data]
            order by 
                event_date desc
        """)

          
        while True:
            result = cursor.fetchall()
            if result:
                print('first',result[0][0])
            if not cursor.nextset():
                break

latest_date = result[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_date = latest_date

print(latest_date)
print(type(latest_date))

end_date = date.today()
latest_date = end_date - timedelta(days=1)
print(latest_date)
print(type(latest_date))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={latest_date}&endtime={end_date}"
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2025-12-26&endtime=2026-05-01"
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
        "longitude": float(item.get("geometry",{}).get("coordinates",[None,None,None])[0] or 0.0),
        "latitude": float(item.get("geometry",{}).get("coordinates",[None,None,None])[1] or 0.0),
        "elevation": float(item.get("geometry",{}).get("coordinates",[None,None,None])[2] or 0.0),
        "title": item.get("properties",{}).get("title",None),
        "magnitude": float(item.get("properties",{}).get("mag") or 0.0),
        "place_description": item.get("properties",{}).get("place",None),
        "sig": item.get("properties",{}).get("sig",None),
        "magType": item.get("properties",{}).get("magType",None),
        "time": item.get("properties",{}).get("time",None),
        "updated": item.get("properties",{}).get("updated",None),
    }
    for item in parsed_data
]

df = spark.createDataFrame(flat_data, schema=schema)
display(df)
#df.write.mode("overwrite").format("delta").saveAsTable(bronze_lh)  

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
