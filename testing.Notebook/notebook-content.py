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
# META         },
# META         {
# META           "id": "75217bdf-bcf2-473e-a349-eb69f5f5a989"
# META         },
# META         {
# META           "id": "e90bbc97-3dcd-42db-9c1c-80268d3dd954"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

dfg = spark.read.table("Gold_LH.Gold_data")
dfs = spark.read.table("Silver_LH.Silver_data")
dfb = spark.read.table("Bronze_LH.bronze_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfb)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import desc
from datetime import date, timedelta, datetime

display(dfg.filter(dfg.event_date == date.today() - timedelta(1)).sort(desc(dfg.event_time)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(date.today() - timedelta(1))

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
