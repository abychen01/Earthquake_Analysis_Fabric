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
# META     }
# META   }
# META }

# CELL ********************

import pyodbc
from pyspark.sql.types import StructType, StringType, StructType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_df = spark.sql("SHOW TABLES")
table_list = [row['tableName'] for row in tables_df.collect()]
db = "earthquake_analysis"

password = spark.read.parquet("Files/creds").collect()[0]['password']

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com,1433;"
            f"DATABASE=master;"
            f"UID=admin;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com,1433;"
            f"DATABASE={db};"
            f"UID=admin;"
            f"PWD={password};"
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


with pyodbc.connect(conn_str_master, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
        
            IF NOT EXISTS (SELECT name from sys.databases WHERE name = ?)
                BEGIN
                SELECT ? + ' doesnt exists';
                EXEC('CREATE DATABASE [' + ? + ']')
                SELECT ? + ' Database created';
                END
            ELSE
                BEGIN
                SELECT ? + ' exist';
                END
        
        """,db,db,db,db,db)

        
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

# CELL ********************

jdbc_url = f"jdbc:sqlserver://fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com:1433;\
            databaseName={db};encrypt=true;trustServerCertificate=true"
jdbc_properties = {
    "user": "admin",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


for table in table_list:

    try:
        df = spark.read.table(table)
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
            .mode("overwrite") \
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

# CELL ********************

#testing....


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
