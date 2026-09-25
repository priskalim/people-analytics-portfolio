# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver — Qualidade e padronização
# MAGIC Nesta etapa, padronizo os nomes das colunas, removo registros sem identificador e duplicatas por colaborador e crio variáveis derivadas para análise de attrition e apresentação dos indicadores.
# MAGIC
# MAGIC
# MAGIC ## Silver — Dicionário de funções PySpark
# MAGIC
# MAGIC | Código ou função | O que faz |
# MAGIC |---|---|
# MAGIC | `spark.table(...)` | Lê a tabela Bronze como um DataFrame. |
# MAGIC | `filter(...)` | Mantém apenas linhas com identificador do colaborador preenchido. |
# MAGIC | `dropDuplicates(["EmployeeNumber"])` | Remove duplicatas usando o identificador do colaborador. |
# MAGIC | `to_snake_case(...)` | Padroniza nomes de colunas, como `EmployeeNumber` para `employee_number`. |
# MAGIC | `withColumnRenamed(...)` | Aplica o novo nome padronizado à coluna. |
# MAGIC | `trim(...)` | Remove espaços extras no início e no fim dos campos de texto. |
# MAGIC | `withColumn(...)` | Cria ou atualiza uma coluna no DataFrame. |
# MAGIC | `when(...).otherwise(...)` | Aplica condições para criar a flag de attrition e os rótulos curtos. |
# MAGIC | `attrition_flag` | Indica attrition com `1` para `Yes` e `0` para `No`. |
# MAGIC | `department_label` | Exibe nomes curtos de departamento, como `R&D` e `HR`. |
# MAGIC | `business_travel_label` | Exibe rótulos curtos para frequência de viagens. |
# MAGIC | `job_role_label` | Exibe nomes curtos para os cargos. Os valores originais são mantidos. |
# MAGIC | `write.format("delta")` | Define Delta como formato de armazenamento. |
# MAGIC | `mode("overwrite")` | Substitui a tabela Silver quando a etapa é executada novamente. |
# MAGIC | `saveAsTable(...)` | Grava o resultado como tabela no catálogo do Databricks. |
# MAGIC | `groupBy(...).count()` | Conta os registros em cada categoria para conferir os rótulos. |
# MAGIC
# MAGIC **Origem:** `workspace.default.employees_bronze`  
# MAGIC **Destino:** `workspace.default.employees_silver`

# COMMAND ----------

# Reload Silver table

df_silver = spark.table(
    "workspace.default.employees_silver"
)

print(f"Registros Silver: {df_silver.count()}")

# COMMAND ----------

from pyspark.sql import functions as F
import re

# 1. Ler a tabela Bronze
df_bronze = spark.table("workspace.default.employees_bronze")
total_bronze = df_bronze.count()

# 2. Remover linhas sem identificador e duplicatas de colaborador
df_silver = (
    df_bronze
    .filter(F.col("EmployeeNumber").isNotNull())
    .dropDuplicates(["EmployeeNumber"])
)

# 3. Padronizar nomes de colunas para snake_case
def to_snake_case(nome):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", nome).lower()

for nome_antigo in df_silver.columns:
    nome_novo = to_snake_case(nome_antigo)
    if nome_antigo != nome_novo:
        df_silver = df_silver.withColumnRenamed(nome_antigo, nome_novo)

# 4. Remover espaços extras dos campos de texto
for campo, tipo in df_silver.dtypes:
    if tipo == "string":
        df_silver = df_silver.withColumn(campo, F.trim(F.col(campo)))

# 5. Criar uma flag numérica para attrition
df_silver = df_silver.withColumn(
    "attrition_flag",
    F.when(F.upper(F.col("attrition")) == "YES", 1)
     .when(F.upper(F.col("attrition")) == "NO", 0)
     .otherwise(None)
)

# 6. Gravar como tabela Delta Silver
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.employees_silver")
)

# 7. Conferir o resultado
total_silver = spark.table("workspace.default.employees_silver").count()

print(f"Registros na Bronze: {total_bronze}")
print(f"Registros na Silver: {total_silver}")
display(spark.table("workspace.default.employees_silver").limit(10))

# COMMAND ----------

display(
    df_silver
    .groupBy("attrition", "attrition_flag")
    .count()
    .orderBy("attrition")
)

# COMMAND ----------

print(f"Total de colaboradores: {df_silver.count()}")

print(
    f"Total de attritions: "
    f"{df_silver.filter(df_silver.attrition_flag == 1).count()}"
)