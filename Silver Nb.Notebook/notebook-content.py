# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fe007dfc-4948-4081-9212-d0239e478983",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "566db19f-9edd-49ca-a404-7bde0e4bd305",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2f1d805-1016-46b7-8712-028624cbdc9c"
# META         },
# META         {
# META           "id": "0a33465b-6efe-413c-a8ab-aa23bf42e2f9"
# META         },
# META         {
# META           "id": "fe007dfc-4948-4081-9212-d0239e478983"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

bronze_lh = ""
silver_lh = ""
gold_lh = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col, to_timestamp, to_date, date_format, isnull
from pyspark.sql.types import TimestampType

#bronze_lh2 = "Bronze_LH.Bronze_data"

df2 = spark.read.table(bronze_lh)

# Handling nulls
df2 = df2.withColumn("longitude", when(isnull(col("longitude")), 0).otherwise(col("longitude")))\
         .withColumn("latitude", when(isnull(col("latitude")), 0).otherwise(col("latitude")))\
         .withColumn("time", when(df2.time.isNull(), 0).otherwise(df2.time))

# Timestamp conversions
df2 = df2.withColumn("time", ((df2.time) / 1000).cast(TimestampType()))\
         .withColumn("updated", ((df2.updated) / 1000).cast(TimestampType()))

# Date and time formatting
df2 = df2.withColumn("event_date", to_date(to_timestamp(col("time"))))\
         .withColumn("event_time", date_format(to_timestamp(col("time")), "HH:mm:ss:SSS"))\
         .withColumn("updated_date", to_date(to_timestamp(col("updated"))))\
         .withColumn("updated_time", date_format(to_timestamp(col("updated")), "HH:mm:ss:SSS"))

# Removing old columns
df2 = df2.drop("time", "updated")

df2.write.format("delta").mode("overwrite").saveAsTable(silver_lh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
import json

output = {
    "silver_lh": silver_lh,
    "gold_lh": gold_lh
}

mssparkutils.notebook.exit(json.dumps(output))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
