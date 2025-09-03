# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a389b12c-e773-475f-91eb-e98c19bc40da",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "8be724d1-75c7-4a15-9b26-dc6747947d8b",
# META       "known_lakehouses": [
# META         {
# META           "id": "a389b12c-e773-475f-91eb-e98c19bc40da"
# META         },
# META         {
# META           "id": "b6141be4-6ec1-4deb-9e3f-ee8c51de76bb"
# META         },
# META         {
# META           "id": "2f73d050-1f8b-4e9d-bd8e-6640ac335a1c"
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

df = spark.sql("SELECT * FROM Gold_LH.gold_data LIMIT 1000")
display(df)

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
