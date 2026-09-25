# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold — Indicadores de People Analytics
# MAGIC
# MAGIC A camada Gold agrega os dados da Silver para facilitar a análise. A taxa representa a proporção de registros com attrition `Yes` neste dataset.
# MAGIC
# MAGIC ### Funções usadas
# MAGIC
# MAGIC | Função | Uso |
# MAGIC |---|---|
# MAGIC | `spark.table(...)` | Lê a tabela Silver. |
# MAGIC | `groupBy(coluna)` | Agrupa registros por uma categoria. |
# MAGIC | `count("*")` | Conta os registros em cada grupo. |
# MAGIC | `sum("attrition_flag")` | Conta os casos de attrition `Yes`. |
# MAGIC | `avg("attrition_flag") * 100` | Calcula a taxa de attrition em percentual. |
# MAGIC | `round(..., 2)` | Arredonda a taxa para duas casas decimais. |
# MAGIC | `orderBy(F.desc(...))` | Ordena da maior para a menor taxa. |
# MAGIC | `saveAsTable(...)` | Grava o resultado como tabela Delta. |
# MAGIC | `display(...)` | Exibe a tabela no notebook. |
# MAGIC
# MAGIC ### Tabelas Gold
# MAGIC
# MAGIC | Tabela | Agrupamento | Indicadores |
# MAGIC |---|---|---|
# MAGIC | `workforce_gold` | Visão geral | Registros, casos e taxa de attrition |
# MAGIC | `attrition_by_department_gold` | `department` | Registros, casos e taxa por departamento |
# MAGIC | `attrition_by_travel_gold` | `business_travel` | Registros, casos e taxa por frequência de viagem |
# MAGIC | `attrition_by_role_gold` | `job_role` | Registros, casos e taxa por cargo |
# MAGIC | `attrition_by_overtime_gold` | `over_time` | Registros, casos e taxa por horas extras |
# MAGIC
# MAGIC ### Campos de saída
# MAGIC
# MAGIC | Campo | Significado |
# MAGIC |---|---|
# MAGIC | `employee_records` | Registros no grupo |
# MAGIC | `attrition_cases` | Registros com attrition `Yes` |
# MAGIC | `attrition_rate_pct` | Percentual de registros com attrition `Yes` |
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table(
    "workspace.default.employees_silver"
)

print(f"Silver records: {df_silver.count()}")

# COMMAND ----------

workforce_gold = (
    df_silver
    .agg(
        F.count("*").alias("employee_records"),
        F.sum("attrition_flag").alias("attrition_cases"),
        F.round(
            F.avg("attrition_flag") * 100,
            2
        ).alias("attrition_rate_pct")
    )
)

display(workforce_gold)

# COMMAND ----------

workforce_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.default.workforce_gold"
    )

# COMMAND ----------

display(
    spark.table(
        "workspace.default.workforce_gold"
    )
)

# COMMAND ----------

attrition_by_overtime_gold = (
    df_silver
    .groupBy("over_time")
    .agg(
        F.count("*").alias("employee_records"),
        F.sum("attrition_flag").alias("attrition_cases"),
        F.round(
            F.avg("attrition_flag") * 100,
            2
        ).alias("attrition_rate_pct")
    )
    .orderBy(F.desc("attrition_rate_pct"))
)

display(attrition_by_overtime_gold)

# COMMAND ----------

attrition_by_overtime_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.default.attrition_by_overtime_gold"
    )

# COMMAND ----------

display(
    spark.table(
        "workspace.default.attrition_by_overtime_gold"
    )
)

# COMMAND ----------

attrition_by_travel_gold = (
    df_silver
    .groupBy("business_travel")
    .agg(
        F.count("*").alias("employee_records"),
        F.sum("attrition_flag").alias("attrition_cases"),
        F.round(
            F.avg("attrition_flag") * 100,
            2
        ).alias("attrition_rate_pct")
    )
    .orderBy(F.desc("attrition_rate_pct"))
)

display(attrition_by_travel_gold)

# COMMAND ----------

attrition_by_travel_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.default.attrition_by_travel_gold"
    )

# COMMAND ----------

display(
    spark.table(
        "workspace.default.attrition_by_travel_gold"
    )
)

# COMMAND ----------

attrition_by_department_gold = (
    df_silver
    .groupBy("department")
    .agg(
        F.count("*").alias("employee_records"),
        F.sum("attrition_flag").alias("attrition_cases"),
        F.round(
            F.avg("attrition_flag") * 100,
            2
        ).alias("attrition_rate_pct")
    )
    .orderBy(F.desc("attrition_rate_pct"))
)

display(attrition_by_department_gold)

# COMMAND ----------

attrition_by_department_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.default.attrition_by_department_gold"
    )

# COMMAND ----------

display(
    spark.table(
        "workspace.default.attrition_by_department_gold"
    )
)

# COMMAND ----------

attrition_by_role_gold = (
    df_silver
    .groupBy("job_role")
    .agg(
        F.count("*").alias("employee_records"),
        F.sum("attrition_flag").alias("attrition_cases"),
        F.round(
            F.avg("attrition_flag") * 100,
            2
        ).alias("attrition_rate_pct")
    )
    .orderBy(F.desc("attrition_rate_pct"))
)

display(attrition_by_role_gold)

# COMMAND ----------

attrition_by_role_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.default.attrition_by_role_gold"
    )

# COMMAND ----------

display(
    spark.table(
        "workspace.default.attrition_by_role_gold"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusão — Gold Layer
# MAGIC
# MAGIC A camada Gold transforma os dados tratados da Silver em indicadores de People Analytics orientados ao negócio.
# MAGIC
# MAGIC Foram criadas cinco tabelas analíticas:
# MAGIC
# MAGIC - `workforce_gold` — visão geral da população analisada
# MAGIC - `attrition_by_department_gold` — attrition por departamento
# MAGIC - `attrition_by_travel_gold` — attrition por frequência de viagens
# MAGIC - `attrition_by_overtime_gold` — attrition por horas extras
# MAGIC - `attrition_by_role_gold` — attrition por cargo
# MAGIC
# MAGIC ## Principais indicadores
# MAGIC
# MAGIC - **Employees:** 1,470
# MAGIC - **Attritions:** 237
# MAGIC - **Observed Attrition Rate:** 16.12%
# MAGIC
# MAGIC ## Business Interpretation
# MAGIC
# MAGIC The analysis identifies differences in observed attrition across employee groups.
# MAGIC
# MAGIC The strongest observed differences in this dataset are associated with:
# MAGIC
# MAGIC - Overtime
# MAGIC - Business Travel
# MAGIC - Department
# MAGIC - Job Role
# MAGIC
# MAGIC These results represent associations observed in the dataset and should not be interpreted as evidence of causality.
# MAGIC
# MAGIC The Gold layer is designed to support further analysis, dashboarding and future predictive People Analytics initiatives.
# MAGIC
# MAGIC > **Dataset note:** The IBM HR Analytics dataset is fictional/educational. The attrition rate represents the proportion of records with `Attrition = Yes` and is not a period-based turnover rate.

# COMMAND ----------

print("=== VALIDAÇÃO FINAL DO PROJETO ===")

print(f"Employees: {spark.table('workspace.default.workforce_gold').first()['employee_records']}")
print(f"Attritions: {spark.table('workspace.default.workforce_gold').first()['attrition_cases']}")
print(f"Attrition Rate: {spark.table('workspace.default.workforce_gold').first()['attrition_rate_pct']}%")

print("\nGold tables:")
print("- workforce_gold")
print("- attrition_by_department_gold")
print("- attrition_by_travel_gold")
print("- attrition_by_overtime_gold")
print("- attrition_by_role_gold")

print("\nProjeto Gold validado com sucesso.")