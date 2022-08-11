# Databricks notebook source
from pyspark.sql.functions import lit
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

# File location and type

name = 'vanessa'
file_location =  '/mnt/raw/autoloader/input/'
database = 'bronze'
delimiter = ","
file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"

path_bronze = f"/mnt/bronze/crime_data_{name}"
count = 0
for i in dbutils.fs.ls('/mnt/raw/autoloader/input'):
    file_name = i[1]
    location = file_location+file_name
 
    df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(location)
  
    df = df.withColumn('file_name',lit(file_name))
    
    if count == 0:
        df_final = df
    else:
        df_final = df.unionAll(df_final)
    print(df_final.count())  
    

# display(df)

# COMMAND ----------

display(df_final)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save(path_bronze)

# COMMAND ----------

table_name = "crimes_"+ name
spark.sql(f"create database if not exists {database}")
spark.sql(f"drop table if exists bronze.{table_name}")
spark.sql(f"create table bronze.{table_name} using delta location '{path_bronze}'")

# COMMAND ----------

print(table_name)
