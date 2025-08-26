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

lists = [
    "gold_data", "Date"
]   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

lists = ["gold_data", "Date"]

for table_name in lists:
    # Read table and coalesce to 1 partition for single output file
    df = spark.read.table(table_name).coalesce(1)
    
    # Temporary directory path
    temp_dir = f"Files/{table_name}_temp"
    final_path = f"Files/{table_name}.csv"

    # Save as CSV to temp directory
    df.write.mode("overwrite").option("header", True).csv(temp_dir)

    # Get Spark's Hadoop FileSystem
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(temp_dir)

    # Find part file and rename
    files = fs.listStatus(path)
    for file in files:
        name = file.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            fs.rename(file.getPath(), spark._jvm.org.apache.hadoop.fs.Path(final_path))
            break

    # Delete temp directory
    fs.delete(path, True)

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
