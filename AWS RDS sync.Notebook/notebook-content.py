# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e7d74e70-e9c6-4ccf-ada8-fd053439f5e1",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "566db19f-9edd-49ca-a404-7bde0e4bd305",
# META       "known_lakehouses": [
# META         {
# META           "id": "e7d74e70-e9c6-4ccf-ada8-fd053439f5e1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Imports

# CELL ********************

import pyodbc, os
from pyspark.sql.types import StructType, StringType, StructType
from pyspark.sql.functions import col, desc, asc
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Declarations

# CELL ********************

tables_df = spark.sql("SHOW TABLES")
table_list = [row['tableName'] for row in tables_df.collect()]
db = "earthquake_analysis"

df_creds = spark.read.parquet('Files/creds')

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]


vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

server_password = client.get_secret("sql-server-password").value

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=master;"
            f"UID=admin2;"
            f"PWD={server_password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE={db};"
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

# MARKDOWN ********************

# #### DB check

# CELL ********************


with pyodbc.connect(conn_str_master, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
        
            IF NOT EXISTS (SELECT name from sys.databases WHERE name = ?)
                BEGIN
                SELECT ? + ' doesnt exists';
                END
            ELSE
                BEGIN
                SELECT ? + ' exist';
                END
        
        """,db,db,db)

        
        while True:
            result = cursor.fetchall()

            if result:    
                print(result)
            if not cursor.nextset():
                break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Table check

# CELL ********************

def converts(datatype):
    datatype = datatype.simpleString()

    match datatype:
        case "int":
            return "INT"
        case "string":
            return "NVARCHAR(255)"  # Using NVARCHAR as requested
        case "timestamp":
            return "DATETIME"
        case "double":
            return "FLOAT"
        case "boolean":
            return "BIT"
        case "decimal":
            return "DECIMAL(18,2)"
        case _:
            return "NVARCHAR(255)"  # Default for unsupported types

for table in table_list:

    print(table)
    df = spark.read.table(table)
    if table == "Date":
        df = df.withColumnsRenamed({"Week of year": "week_of_year","Day Name": "day_name"})
    table_cols = [f"{field.name} {converts(field.dataType)}" for field in df.schema.fields]   

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                IF NOT EXISTS (SELECT name FROM sys.tables WHERE name = ?)
                    BEGIN
                    SELECT '['+ ? + '] doesnt exist'
                    EXEC('CREATE TABLE [' + ? + '] (' + ? + ')')
                    SELECT '['+ ? + '] created' 
                    END
                ELSE
                    BEGIN
                    SELECT '[' + ? + '] exists already'
                    END

            """,table,table,table,','.join(table_cols),table,table)

            while True:
                result = cursor.fetchall()
                if result:
                    print(result[0])   
                if not cursor.nextset():
                    break


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Write

# CELL ********************

with pyodbc.connect(conn_str, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT TOP(1) event_date FROM gold_data order by event_date desc")

        while True:
            latest_date = cursor.fetchall()
            latest_date = latest_date[0][0]

            if result:    
                print(latest_date)
            if not cursor.nextset():
                break


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbc_url = "jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           f"databaseName={db};" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": server_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

for table in table_list:

    try:
        df = spark.read.table(table)
        if table == 'gold_data':
            df = spark.read.table('gold_data').where(col('event_date') > latest_date)
        if table == "Date":
            df = df.withColumnsRenamed({"Week of year": "week_of_year","Day Name": "day_name"})

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode("append") \
            .save()
        print(f"Successfully wrote data to RDS table '{table}'.")


    except Exception as e:
        print(f"Failed to write to RDS: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Testing

# CELL ********************

df = spark.read.table('gold_data').where(col('event_date') == '2025-11-02')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#testing....
'''

for table in table_list:
    

    with pyodbc.connect(conn_str,autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                
                EXEC('SELECT count(*) FROM [' + ? + ']'
                
            
            )""",table)
            print(table)
            while True:
                result = cursor.fetchall()
                if result:
                    print(result[0])
                if not cursor.nextset():
                    break


'''

jdbc_url = "jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           f"databaseName={db};" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": server_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

table = 'gold_data'
try:
    df = spark.read.table(table)
    if table == 'gold_data':
        df = spark.read.table('gold_data').where(col('event_date') == '2025-11-02')

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .option("batchsize", 1000) \
        .mode("append") \
        .save()
    print(f"Successfully wrote data to RDS table '{table}'.")


except Exception as e:
    print(f"Failed to write to RDS: {e}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
