# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Data Ingestion
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the IBM HR Analytics Employee Attrition & Performance dataset into the Bronze layer of the Databricks Lakehouse.
# MAGIC
# MAGIC The Bronze layer preserves the source data with minimal transformation while adding an ingestion timestamp for traceability.
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC - Dataset: IBM HR Analytics Employee Attrition & Performance
# MAGIC - Records: 1,470
# MAGIC - Format: Delta Table
# MAGIC
# MAGIC ## Process
# MAGIC
# MAGIC 1. Read the source employee table
# MAGIC 2. Add ingestion timestamp
# MAGIC 3. Persist the data as a Delta table
# MAGIC 4. Validate record count and sample records

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# 1. Ler a tabela que já está no Databricks
df_origem = spark.table("workspace.default.wa_fn_use_c_hr_employee_attrition")

# 2. Acrescentar o horário da ingestão
df_bronze = df_origem.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# 3. Gravar a camada Bronze como tabela Delta
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.employees_bronze")
)

# 4. Conferir o resultado
print(f"Registros gravados: {spark.table('workspace.default.employees_bronze').count()}")
display(spark.table("workspace.default.employees_bronze").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC The Bronze table was successfully created with 1,470 employee records.
# MAGIC
# MAGIC The `ingestion_timestamp` column provides basic traceability of when the data was ingested into the Bronze layer.