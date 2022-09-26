# Databricks notebook source
# MAGIC %md
# MAGIC ## When was my table last used?
# MAGIC 
# MAGIC **Purpose**: Use this notebook to determine when tables were last used within a workspace.
# MAGIC 
# MAGIC _Note_: Please comment out Line 4 of Cell 5 to run this notebook on all databases and tables in a workspace. By default, the notebook will check the first 5 databases only.

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;

# COMMAND ----------

def is_delta_table(table_name):
  """Helper function to determine if table is in Delta format.
  @table_name: string - fully qualified name of table
  @returns: bool - true/false if table is in Delta format
  """
  provider = spark.sql(f"describe extended {table_name}").where("col_name='Provider'").collect()[0]
  return provider['data_type'] == 'delta'

# COMMAND ----------

def get_last_used_timestamp(table_name):
  """Helper function that gets the last used timestamp from a Delta table history
  @table_name: string - the fully qualified table name of the Delta table
  @returns: string - the timestamp that the Delta table was last modified
  """
  return spark.sql(f"""select CAST(max(timestamp) as STRING) as `last_used_timestamp`
    from (
       describe history {table_name}
       )""").collect()[0]['last_used_timestamp']


# COMMAND ----------

all_tables = []
databases = [r['databaseName'] for r in spark.sql("show databases").collect()]
# comment out to run on the full set of databases and tables
databases[0:5]
for database in databases:
  tables = [r['tableName'] for r in spark.sql(f"show tables in {database}").where("isTemporary=false").collect()]
  for table in tables:
    try:
      full_table_name = f'{database}.{table}'
      delta_format = is_delta_table(full_table_name)
      if delta_format:
        last_used_timestamp = get_last_used_timestamp(full_table_name)
      else:
        last_used_timestamp = 'UNKNOWN'
      table_info = {'database': database, 'table': table, 'delta_format': delta_format, 'last_used_timestamp': last_used_timestamp}
      all_tables.append(table_info)
    except Exception as e:
      print(e)

# COMMAND ----------

all_tables_df = spark.createDataFrame(all_tables, 'database string, table string, delta_format boolean, last_used_timestamp string')
all_tables_df.display()

# COMMAND ----------


