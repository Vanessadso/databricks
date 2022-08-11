# Databricks notebook source
table_name = dbutils.widgets.get("table_name")
df = spark.sql(f"select * from {table_name}")
display(df)
