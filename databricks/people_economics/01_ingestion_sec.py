# Databricks notebook source
# MAGIC %md
# MAGIC # People Economics — Análise de investimento em pessoas
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivo
# MAGIC
# MAGIC Este projeto tem como objetivo analisar a relação entre o investimento
# MAGIC realizado pelas empresas em sua força de trabalho e seu desempenho
# MAGIC operacional.
# MAGIC
# MAGIC Serão analisadas cinco empresas públicas dos Estados Unidos:
# MAGIC
# MAGIC - Costco Wholesale Corporation
# MAGIC - Microsoft Corporation
# MAGIC - Starbucks Corporation
# MAGIC - United Parcel Service, Inc.
# MAGIC - Walmart Inc.
# MAGIC
# MAGIC Os dados financeiros serão obtidos a partir dos Financial Statement
# MAGIC Data Sets da SEC e posteriormente integrados a informações de
# MAGIC workforce, compensation e benefits.
# MAGIC
# MAGIC Fonte: https://www.sec.gov/dera/data/financial-statement-data-sets.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta de negócio
# MAGIC
# MAGIC Como diferentes empresas convertem o investimento realizado em sua
# MAGIC força de trabalho em desempenho operacional?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hipóteses / questões de investigação
# MAGIC
# MAGIC - Empresas com diferentes modelos de negócio apresentam diferentes
# MAGIC   relações entre People Cost e Revenue?
# MAGIC
# MAGIC - Como o investimento em pessoas se relaciona com o Operating Income?
# MAGIC
# MAGIC - Como Revenue per Employee e Operating Income per Employee variam
# MAGIC   entre os diferentes modelos de negócio?
# MAGIC
# MAGIC - Quais fatores podem ajudar a explicar as diferenças observadas
# MAGIC   entre as empresas?
# MAGIC
# MAGIC Nesta etapa inicial, as questões serão investigadas de forma
# MAGIC descritiva. Diferenças observadas entre empresas não serão
# MAGIC interpretadas automaticamente como relações causais.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indicadores
# MAGIC
# MAGIC ### Indicador principal
# MAGIC
# MAGIC People ROI
# MAGIC
# MAGIC People ROI = Operating Income / Total People Cost
# MAGIC
# MAGIC ### Indicadores complementares
# MAGIC
# MAGIC - People Cost / Revenue
# MAGIC - Revenue / Employee
# MAGIC - Operating Income / Employee
# MAGIC - People ROI

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Exploração e entendimento dos dados
# MAGIC
# MAGIC Os dados financeiros são provenientes dos Financial Statement Data Sets
# MAGIC da SEC. Foram utilizados os datasets 2025 Q3, 2025 Q4 e 2026 Q1 para
# MAGIC contemplar o exercício fiscal completo das cinco empresas, que possuem
# MAGIC diferentes datas de encerramento.
# MAGIC
# MAGIC Os arquivos principais utilizados são:
# MAGIC
# MAGIC - `SUB`: identificação dos filings
# MAGIC - `PRE`: estrutura das demonstrações
# MAGIC - `NUM`: valores financeiros
# MAGIC - `TAG`: metadados das tags XBRL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Localização dos dados
# MAGIC
# MAGIC Os arquivos foram armazenados na camada Bronze do projeto, em um Unity
# MAGIC Catalog Volume, preservando os dados originais antes das transformações.

# COMMAND ----------

from pyspark.sql.functions import col, lit

BASE_PATH = "/Volumes/workspace/default/people_economics_bronze"

QUARTERS = ["2025q3", "2025q4", "2026q1"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Identificação dos filings
# MAGIC
# MAGIC O arquivo `SUB` será utilizado para identificar os filings 10-K das
# MAGIC cinco empresas e seus respectivos períodos fiscais.

# COMMAND ----------

sub_dfs = []

for quarter in QUARTERS:
    df = (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .csv(f"{BASE_PATH}/{quarter}/sub.txt")
        .withColumn("source_quarter", lit(quarter))
    )
    
    sub_dfs.append(df)

df_sub = sub_dfs[0]

for df in sub_dfs[1:]:
    df_sub = df_sub.unionByName(df)

print(f"Registros carregados: {df_sub.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Seleção dos filings anuais
# MAGIC
# MAGIC O arquivo `SUB` reúne informações de diferentes empresas e tipos de
# MAGIC filing.
# MAGIC
# MAGIC Nesta etapa serão selecionados apenas os relatórios anuais (`10-K`)
# MAGIC das cinco empresas analisadas.
# MAGIC
# MAGIC O `ADSH` de cada filing será utilizado posteriormente para relacionar
# MAGIC o relatório às informações financeiras do arquivo `NUM`.

# COMMAND ----------

EMPRESAS = [
    "COSTCO",
    "MICROSOFT",
    "STARBUCKS",
    "UNITED PARCEL SERVICE",
    "WALMART"
]

filtro_empresas = " OR ".join(
    [f"upper(name) LIKE '%{empresa}%'" for empresa in EMPRESAS]
)

df_10k = (
    df_sub
    .filter(filtro_empresas)
    .filter(col("form") == "10-K")
)

display(
    df_10k.select(
        "cik",
        "name",
        "form",
        "period",
        "fy",
        "fp",
        "filed",
        "adsh",
        "source_quarter"
    )
    .orderBy("name", "period")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Estrutura das demonstrações financeiras
# MAGIC
# MAGIC O arquivo `PRE` apresenta a estrutura das demonstrações financeiras
# MAGIC reportadas em cada filing.
# MAGIC
# MAGIC Nesta etapa, vamos identificar as linhas da demonstração de resultado
# MAGIC que correspondem à Receita e ao Operating Income.
# MAGIC
# MAGIC Essas informações serão utilizadas posteriormente para localizar os
# MAGIC valores corretos no arquivo `NUM`.

# COMMAND ----------

pre_dfs = []

for quarter in QUARTERS:
    df = (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .csv(f"{BASE_PATH}/{quarter}/pre.txt")
        .withColumn("source_quarter", lit(quarter))
    )
    
    pre_dfs.append(df)

df_pre = pre_dfs[0]

for df in pre_dfs[1:]:
    df_pre = df_pre.unionByName(df)

print(f"Registros carregados: {df_pre.count():,}")

# COMMAND ----------

adsh_10k = [row["adsh"] for row in df_10k.select("adsh").collect()]

df_pre_10k = (
    df_pre
    .filter(col("adsh").isin(adsh_10k))
)

display(df_pre_10k.limit(50))

# COMMAND ----------

df_pre_finance = (
    df_pre_10k
    .filter(
        (col("stmt") == "IS") &
        (
            col("tag").isin([
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "Revenues",
                "SalesRevenueNet",
                "OperatingIncomeLoss",
                "OperatingIncome"
            ])
        )
    )
)

display(
    df_pre_finance.select(
        "adsh",
        "report",
        "line",
        "stmt",
        "tag",
        "plabel"
    )
    .orderBy("adsh", "line")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretação
# MAGIC
# MAGIC A estrutura das demonstrações varia entre as empresas. Embora
# MAGIC `OperatingIncomeLoss` seja utilizada pelas cinco empresas para
# MAGIC Operating Income, a Receita pode ser representada por diferentes tags.
# MAGIC
# MAGIC Por isso, a seleção dos valores será baseada na estrutura identificada
# MAGIC no `PRE`, e não apenas em uma única tag aplicada indiscriminadamente
# MAGIC a todas as empresas.
# MAGIC
# MAGIC No caso do Walmart, será utilizada a linha `Total revenues`, em vez de
# MAGIC `Net sales`, para representar a receita consolidada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Valores financeiros
# MAGIC
# MAGIC Com as linhas financeiras identificadas no `PRE`, será utilizado o
# MAGIC arquivo `NUM` para recuperar os valores reportados pelas empresas.
# MAGIC
# MAGIC Serão considerados os valores anuais correspondentes ao exercício
# MAGIC fiscal completo de cada 10-K.

# COMMAND ----------

num_dfs = []

for quarter in QUARTERS:
    df = (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .csv(f"{BASE_PATH}/{quarter}/num.txt")
        .filter(col("adsh").isin(adsh_10k))
    )
    
    num_dfs.append(df)

df_num = num_dfs[0]

for df in num_dfs[1:]:
    df_num = df_num.unionByName(df)

print(f"Registros financeiros selecionados: {df_num.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Seleção dos valores financeiros
# MAGIC
# MAGIC A partir das tags identificadas no `PRE`, serão selecionados no `NUM`
# MAGIC os registros correspondentes a Revenue e Operating Income.
# MAGIC
# MAGIC A seleção será restrita aos cinco filings anuais previamente definidos.

# COMMAND ----------

tags_financeiras = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "OperatingIncomeLoss"
]

df_financial = (
    df_num
    .filter(col("tag").isin(tags_financeiras))
)

display(
    df_financial.select(
        "adsh",
        "tag",
        "ddate",
        "qtrs",
        "uom",
        "value"
    ).orderBy("adsh", "ddate", "tag")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6.1 Validação do período financeiro
# MAGIC
# MAGIC O arquivo `NUM` pode conter valores referentes ao exercício atual e a
# MAGIC períodos comparativos apresentados no mesmo filing.
# MAGIC
# MAGIC Por isso, o valor selecionado deverá corresponder à data de encerramento
# MAGIC do exercício fiscal identificada no arquivo `SUB`.

# COMMAND ----------

df_periodos = (
    df_10k
    .select(
        "adsh",
        "cik",
        "name",
        "period",
        "fy"
    )
)

df_financial_period = (
    df_financial
    .join(
        df_periodos,
        on="adsh",
        how="inner"
    )
    .filter(col("ddate") == col("period"))
    .filter(col("qtrs") == "4")
)

display(
    df_financial_period
    .select(
        "name",
        "fy",
        "period",
        "tag",
        "ddate",
        "qtrs",
        "uom",
        "value"
    )
    .orderBy("name", "tag", "value")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6.2 Identificação do registro consolidado
# MAGIC
# MAGIC O arquivo `NUM` pode apresentar diferentes valores para uma mesma tag,
# MAGIC período e unidade, quando existem informações segmentadas.
# MAGIC
# MAGIC Na versão atual dos Financial Statement Data Sets, o campo `segments`
# MAGIC permite identificar se o valor está associado a uma segmentação.
# MAGIC
# MAGIC Para esta análise, serão priorizados os registros sem segmentação,
# MAGIC correspondentes ao valor consolidado da empresa.

# COMMAND ----------

display(
    df_financial_period
    .select(
        "name",
        "tag",
        "ddate",
        "qtrs",
        "uom",
        "segments",
        "value"
    )
    .orderBy("name", "tag", "value")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6.2 Identificação dos valores consolidados
# MAGIC
# MAGIC O campo `segments` identifica valores associados a segmentos ou dimensões específicas. Para esta análise, serão considerados os registros sem segmentação, que representam o valor consolidado reportado pela companhia.

# COMMAND ----------

from pyspark.sql.functions import trim

df_financial_consolidated = (
    df_financial_period
    .filter(
        col("segments").isNull() |
        (trim(col("segments")) == "")
    )
)

display(
    df_financial_consolidated
    .select(
        "name",
        "tag",
        "ddate",
        "qtrs",
        "uom",
        "segments",
        "value"
    )
    .orderBy("name", "tag")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6.3 Definição dos valores financeiros
# MAGIC
# MAGIC Após a identificação dos registros consolidados, será aplicada a regra de seleção das métricas.
# MAGIC
# MAGIC Para Revenue, será utilizada a tag `Revenues` quando disponível. Nos demais casos, será utilizada `RevenueFromContractWithCustomerExcludingAssessedTax`.
# MAGIC
# MAGIC Para Operating Income, será utilizada `OperatingIncomeLoss`.

# COMMAND ----------

df_financial_final = (
    df_financial_consolidated
    .filter(
        (col("tag") == "OperatingIncomeLoss") |
        (col("tag") == "Revenues") |
        (
            (col("tag") == "RevenueFromContractWithCustomerExcludingAssessedTax") &
            (col("name") != "WALMART INC.")
        )
    )
)

display(
    df_financial_final
    .select(
        "name",
        "tag",
        "ddate",
        "qtrs",
        "uom",
        "value"
    )
    .orderBy("name", "tag")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Consolidação dos indicadores financeiros
# MAGIC
# MAGIC Com os valores de Revenue e Operating Income definidos, os dados serão reorganizados para obter uma única linha por empresa. Essa estrutura facilitará a integração posterior com Headcount, Compensation e Benefits.

# COMMAND ----------

from pyspark.sql.functions import first, when

df_financial_base = (
    df_financial_final
    .withColumn(
        "metric",
        when(col("tag") == "OperatingIncomeLoss", "operating_income")
        .otherwise("revenue")
    )
    .groupBy(
        "name",
        "ddate",
        "qtrs",
        "uom"
    )
    .pivot("metric", ["revenue", "operating_income"])
    .agg(first("value"))
)

display(
    df_financial_base
    .orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC A base financeira consolidada contém os valores anuais de Revenue e Operating Income das cinco empresas.
# MAGIC
# MAGIC Esses valores serão utilizados posteriormente como referência para relacionar desempenho operacional ao investimento em pessoas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Headcount

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Headcount
# MAGIC
# MAGIC O Headcount foi obtido nos respectivos Form 10-K das cinco empresas, considerando o número de empregados informado no encerramento do exercício fiscal correspondente.
# MAGIC
# MAGIC Como essa informação é divulgada diretamente pelas companhias e não possui uma estrutura padronizada equivalente ao Revenue e Operating Income no dataset NUM, ela será incorporada como uma tabela de referência.

# COMMAND ----------

headcount_data = [
    ("COSTCO WHOLESALE CORP /NEW", 2025, "2025-08-31", 341000),
    ("MICROSOFT CORP", 2025, "2025-06-30", 228000),
    ("STARBUCKS CORP", 2025, "2025-09-28", 381000),
    ("UNITED PARCEL SERVICE INC", 2025, "2025-12-31", 460000),
    ("WALMART INC.", 2026, "2026-01-31", 2100000)
]

df_headcount = spark.createDataFrame(
    headcount_data,
    ["name", "fy", "headcount_date", "headcount"]
)

display(df_headcount.orderBy("name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 Integração Financeiro + Headcount
# MAGIC
# MAGIC A base financeira será integrada ao Headcount por empresa e exercício fiscal. O resultado reunirá os principais dados necessários para analisar a produtividade econômica da força de trabalho.

# COMMAND ----------

from pyspark.sql.functions import to_date, year

df_financial_headcount = (
    df_financial_base
    .withColumn(
        "fy",
        year(to_date(col("ddate").cast("string"), "yyyyMMdd"))
    )
    .join(
        df_headcount,
        on=["name", "fy"],
        how="inner"
    )
)

display(
    df_financial_headcount
    .select(
        "name",
        "fy",
        "revenue",
        "operating_income",
        "headcount"
    )
    .orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1.10 Indicadores de produtividade
# MAGIC
# MAGIC Com Revenue, Operating Income e Headcount integrados, serão calculados dois indicadores de produtividade econômica da força de trabalho:
# MAGIC
# MAGIC - Revenue per Employee: receita gerada por empregado.
# MAGIC - Operating Income per Employee: lucro operacional gerado por empregado.
# MAGIC
# MAGIC Esses indicadores permitem comparar a relação entre desempenho econômico e tamanho da força de trabalho, sem estabelecer causalidade.

# COMMAND ----------

from pyspark.sql.functions import col, round

df_productivity = (
    df_financial_headcount
    .withColumn(
        "revenue_per_employee",
        round(
            col("revenue").cast("double") /
            col("headcount").cast("double"),
            2
        )
    )
    .withColumn(
        "operating_income_per_employee",
        round(
            col("operating_income").cast("double") /
            col("headcount").cast("double"),
            2
        )
    )
)

display(
    df_productivity
    .select(
        "name",
        "fy",
        "headcount",
        "revenue_per_employee",
        "operating_income_per_employee"
    )
    .orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.11 Interpretação dos indicadores de produtividade
# MAGIC
# MAGIC Os indicadores mostram diferenças relevantes na relação entre tamanho da força de trabalho e desempenho econômico.
# MAGIC
# MAGIC Revenue per Employee representa a receita associada, em termos financeiros, a cada empregado. Operating Income per Employee representa o resultado operacional associado a cada empregado.
# MAGIC
# MAGIC As diferenças entre empresas devem ser interpretadas considerando o modelo de negócio, composição da força de trabalho, intensidade operacional e estrutura de custos. Esses indicadores são descritivos e não demonstram causalidade.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.12 Exploração de Compensation & Benefits
# MAGIC
# MAGIC Diferentemente do Headcount, os custos relacionados às pessoas não são apresentados de forma uniforme pelas cinco empresas.
# MAGIC
# MAGIC Nesta etapa, serão exploradas as tags financeiras disponíveis no dataset SEC para identificar possíveis valores de salários, benefícios, compensação e custos relacionados aos empregados.
# MAGIC
# MAGIC A comparabilidade entre empresas será validada antes da definição de `Total People Cost`.

# COMMAND ----------

tags_people = [
    "LaborAndRelatedExpense",
    "SalariesWagesAndOfficersCompensation",
    "SalariesAndWages",
    "EmployeeBenefitsExpense",
    "ShareBasedCompensation"
]

df_people_tags = (
    df_num
    .filter(col("tag").isin(tags_people))
)

display(
    df_people_tags
    .select(
        "adsh",
        "tag",
        "ddate",
        "qtrs",
        "uom",
        "segments",
        "value"
    )
    .orderBy("adsh", "tag", "ddate")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.12.1 Exploração das tags relacionadas a pessoas
# MAGIC
# MAGIC A primeira busca identificou diferentes formas de divulgação entre as empresas. Será realizada uma busca exploratória por tags relacionadas a salários, benefícios, empregados, compensação e custos trabalhistas.
# MAGIC
# MAGIC O objetivo é identificar todas as métricas potencialmente relevantes antes de definir a metodologia de `Total People Cost`.

# COMMAND ----------

from pyspark.sql.functions import lower

keywords = [
    "employee",
    "compensation",
    "benefit",
    "salary",
    "salaries",
    "wage",
    "labor",
    "payroll"
]

conditions = [
    lower(col("tag")).contains(keyword)
    for keyword in keywords
]

df_people_tags_exploration = (
    df_num
    .filter(
        col("adsh").isin(adsh_10k)
    )
    .filter(
        conditions[0] |
        conditions[1] |
        conditions[2] |
        conditions[3] |
        conditions[4] |
        conditions[5] |
        conditions[6] |
        conditions[7]
    )
    .filter(
        col("qtrs") == "4"
    )
    .filter(
        col("segments").isNull() |
        (col("segments") == "")
    )
)

display(
    df_people_tags_exploration
    .select(
        "adsh",
        "tag",
        "ddate",
        "qtrs",
        "uom",
        "value"
    )
    .orderBy("adsh", "tag", "ddate")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.12.2 Avaliação da comparabilidade
# MAGIC
# MAGIC A exploração do dataset SEC mostrou que as empresas não divulgam Compensation & Benefits de forma padronizada no XBRL.
# MAGIC
# MAGIC Algumas métricas representam apenas componentes específicos da remuneração, enquanto outras aparecem incorporadas em diferentes categorias de despesas.
# MAGIC
# MAGIC Por esse motivo, `ShareBasedCompensation` não será utilizado como proxy de Total People Cost.
# MAGIC
# MAGIC A definição de People Cost será estabelecida somente após avaliar as informações divulgadas nos respectivos 10-Ks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.13 Definição de People Cost
# MAGIC
# MAGIC A análise mostrou que Compensation & Benefits não é divulgado de forma padronizada pelas cinco empresas.
# MAGIC
# MAGIC Por isso, antes de calcular People ROI, será construída uma matriz de disponibilidade das informações de custo de pessoas. Para cada empresa serão avaliados:
# MAGIC
# MAGIC - medida divulgada;
# MAGIC - escopo da medida;
# MAGIC - período;
# MAGIC - unidade;
# MAGIC - abrangência;
# MAGIC - comparabilidade com as demais empresas.
# MAGIC
# MAGIC Somente medidas suficientemente comparáveis serão utilizadas no cálculo de Total People Cost.

# COMMAND ----------

people_cost_assessment = [
    ("COSTCO WHOLESALE CORP /NEW", "Compensation and benefits", 
     "Divulgado como conceito, sem total consolidado", "Não padronizado"),
    
    ("MICROSOFT CORP", "Employee compensation and benefits",
     "Custos distribuídos entre diferentes categorias de despesas", "Não padronizado"),
    
    ("STARBUCKS CORP", "Wages and benefits",
     "Divulgado em componentes específicos das despesas operacionais", "Escopo parcial"),
    
    ("UNITED PARCEL SERVICE INC", "Compensation and Benefits",
     "Valor consolidado divulgado", "Comparável"),
    
    ("WALMART INC.", "Wages and benefits",
     "Custos relacionados a pessoas distribuídos nas despesas", "Não padronizado")
]

df_people_assessment = spark.createDataFrame(
    people_cost_assessment,
    ["name", "measure", "scope", "comparability"]
)

display(df_people_assessment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.13.1 Matriz de disponibilidade de People Cost
# MAGIC
# MAGIC A análise dos 10-Ks mostra diferentes níveis de granularidade na divulgação dos custos relacionados aos empregados.
# MAGIC
# MAGIC Antes de calcular People ROI, será avaliado se existe uma medida de People Cost suficientemente abrangente e comparável entre as empresas.

# COMMAND ----------

people_cost_assessment = [
    ("COSTCO WHOLESALE CORP /NEW", "Compensation and benefits",
     "Distribuído entre Merchandise Costs e SG&A", "Sem total consolidado"),

    ("MICROSOFT CORP", "Payroll, employee benefits e stock-based compensation",
     "Distribuído entre R&D, Sales & Marketing e G&A", "Sem total consolidado"),

    ("STARBUCKS CORP", "Wages and benefits",
     "Store Operating Expenses", "Escopo parcial"),

    ("UNITED PARCEL SERVICE INC", "Compensation and Benefits",
     "Linha consolidada de Operating Expenses", "Total divulgado"),

    ("WALMART INC.", "Wages and benefits",
     "Distribuído na estrutura de custos", "Sem total consolidado")
]

df_people_assessment = spark.createDataFrame(
    people_cost_assessment,
    ["name", "measure", "scope", "availability"]
)

display(df_people_assessment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.14 Identificação de custos de pessoal
# MAGIC
# MAGIC Como não existe uma tag única de People Cost para as cinco empresas, será realizada uma busca nas demonstrações e notas financeiras para identificar valores de salários, benefícios e custos relacionados à força de trabalho.
# MAGIC
# MAGIC A análise considerará o escopo do valor antes de utilizá-lo como People Cost.

# COMMAND ----------

people_terms = [
    "compensation",
    "employee",
    "benefit",
    "wages",
    "salaries",
    "labor",
    "payroll"
]

conditions = [
    lower(col("plabel")).contains(term)
    for term in people_terms
]

df_people_lines = (
    df_pre_10k
    .filter(
        conditions[0] |
        conditions[1] |
        conditions[2] |
        conditions[3] |
        conditions[4] |
        conditions[5] |
        conditions[6]
    )
)

display(
    df_people_lines
    .select(
        "adsh",
        "stmt",
        "line",
        "tag",
        "plabel"
    )
    .orderBy("adsh", "stmt", "line")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.14 Conclusão da exploração de People Cost
# MAGIC
# MAGIC A exploração das demonstrações financeiras mostrou que Compensation & Benefits não é apresentado como uma linha padronizada nas cinco empresas.
# MAGIC
# MAGIC Apenas a UPS apresenta `LaborAndRelatedExpense` diretamente como `Compensation and benefits` na demonstração de resultados. Nas demais empresas, os custos relacionados aos empregados estão distribuídos entre diferentes categorias de despesas ou aparecem como componentes específicos, como stock-based compensation.
# MAGIC
# MAGIC Portanto, o `NUM` e o `PRE` não fornecem uma medida de People Cost diretamente comparável para as cinco empresas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.15 Evidências de People Cost nos 10-K
# MAGIC
# MAGIC A análise dos 10-K mostrou que as empresas não divulgam People Cost com a mesma estrutura contábil.
# MAGIC
# MAGIC Por isso, antes de calcular o People ROI, será documentado:
# MAGIC - qual conceito relacionado a pessoas é divulgado;
# MAGIC - qual é o escopo do valor;
# MAGIC - se existe um valor consolidado;
# MAGIC - e se o valor é comparável entre as empresas.
# MAGIC
# MAGIC Essa validação evita combinar métricas de escopos diferentes e produzir um indicador artificialmente comparável.

# COMMAND ----------

people_cost_evidence = [
    (
        "COSTCO WHOLESALE CORP /NEW",
        "Compensation and benefits",
        "Distribuído entre Merchandise Costs e SG&A",
        None,
        "Sem total consolidado"
    ),
    (
        "MICROSOFT CORP",
        "Payroll, employee benefits e stock-based compensation",
        "Distribuído entre R&D, Sales & Marketing e G&A",
        None,
        "Sem total consolidado"
    ),
    (
        "STARBUCKS CORP",
        "Wages and benefits",
        "Store Operating Expenses",
        9862.4,
        "Escopo parcial"
    ),
    (
        "UNITED PARCEL SERVICE INC",
        "Compensation and Benefits",
        "Operating Expenses — consolidado",
        48605.0,
        "Total divulgado"
    ),
    (
        "WALMART INC.",
        "Wages and benefits",
        "Distribuído na estrutura de custos",
        None,
        "Sem total consolidado"
    )
]

df_people_cost_evidence = spark.createDataFrame(
    people_cost_evidence,
    [
        "name",
        "people_cost_concept",
        "scope",
        "value_millions",
        "comparability"
    ]
)

display(df_people_cost_evidence)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.16 Interpretação da comparabilidade
# MAGIC
# MAGIC Apenas a UPS apresenta uma linha consolidada de Compensation and Benefits diretamente utilizável como People Cost.
# MAGIC
# MAGIC Nas demais empresas, os custos relacionados às pessoas estão distribuídos em diferentes categorias ou apresentam escopo parcial.
# MAGIC
# MAGIC Portanto, não será calculado um People ROI utilizando valores diferentes de People Cost para cada empresa.
# MAGIC
# MAGIC O próximo passo será avaliar se é possível construir uma definição padronizada de People Cost para as cinco empresas a partir das informações disponíveis nos 10-Ks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.17 Definição do People Cost utilizado na análise
# MAGIC
# MAGIC Para garantir comparabilidade e rastreabilidade, o People Cost somente será utilizado no cálculo do People ROI quando houver um valor anual consolidado, com escopo claramente identificado e validado no 10-K.
# MAGIC
# MAGIC Quando essas condições não forem atendidas, o indicador será mantido como não disponível para a empresa.
# MAGIC
# MAGIC Dessa forma, a ausência do indicador representa uma limitação de divulgação dos dados, e não uma estimativa criada para completar a análise.

# COMMAND ----------

people_cost_validated = [
    ("UNITED PARCEL SERVICE INC", 2025, 48605.0, "Compensation and Benefits", "Validated"),
    ("COSTCO WHOLESALE CORP /NEW", 2025, None, None, "Not available"),
    ("MICROSOFT CORP", 2025, None, None, "Not available"),
    ("STARBUCKS CORP", 2025, None, None, "Partial scope"),
    ("WALMART INC.", 2026, None, None, "Not available")
]

df_people_cost_validated = spark.createDataFrame(
    people_cost_validated,
    [
        "name",
        "fy",
        "people_cost_millions",
        "people_cost_concept",
        "validation_status"
    ]
)

display(
    df_people_cost_validated.orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. People Economics — Indicadores finais
# MAGIC
# MAGIC Com os dados financeiros, headcount e People Cost validados, serão calculados os indicadores finais do projeto.
# MAGIC
# MAGIC Os indicadores de produtividade serão apresentados para as cinco empresas.
# MAGIC
# MAGIC O People ROI será calculado somente para empresas cujo People Cost tenha sido validado com escopo consolidado e comparável.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.18 Consulta de conceitos XBRL específicos
# MAGIC
# MAGIC Os Financial Statement Data Sets utilizados na camada Bronze não contemplam todas as informações XBRL disponíveis nos filings.
# MAGIC
# MAGIC Por isso, para conceitos de People Cost que não foram identificados no `NUM`, será realizada uma consulta complementar aos conceitos XBRL da SEC por companhia.
# MAGIC
# MAGIC O objetivo é verificar se existe um conceito de custo de pessoal diretamente reportado e se seu escopo permite utilização no cálculo do People ROI.

# COMMAND ----------

starbucks_labor_2025 = 8828.6

print(f"Starbucks Labor and Related Expense FY2025: US$ {starbucks_labor_2025:,.1f} milhões")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Integração do People Cost
# MAGIC
# MAGIC Os dados de People Cost validados serão integrados à base financeira e de headcount.
# MAGIC
# MAGIC Será utilizado um `left join` para manter as cinco empresas na análise. Quando o People Cost não estiver disponível ou validado, o valor permanecerá nulo.
# MAGIC
# MAGIC Isso permite calcular os indicadores de produtividade para todas as empresas e o People ROI somente para aquelas com People Cost validado.

# COMMAND ----------

people_cost_validated = [
    ("COSTCO WHOLESALE CORP /NEW", 2025, None, None, "Not available"),
    ("MICROSOFT CORP", 2025, None, None, "Not available"),
    ("STARBUCKS CORP", 2025, 8828.6, "Labor and Related Expense", "Validated"),
    ("UNITED PARCEL SERVICE INC", 2025, 48605.0, "Compensation and Benefits", "Validated"),
    ("WALMART INC.", 2026, None, None, "Not available")
]

df_people_cost_validated = spark.createDataFrame(
    people_cost_validated,
    [
        "name",
        "fy",
        "people_cost_millions",
        "people_cost_concept",
        "validation_status"
    ]
)

display(
    df_people_cost_validated
    .orderBy("name")
)

# COMMAND ----------

df_people_economics = (
    df_productivity
    .join(
        df_people_cost_validated,
        on=["name", "fy"],
        how="left"
    )
)

display(
    df_people_economics
    .select(
        "name",
        "fy",
        "revenue",
        "operating_income",
        "headcount",
        "people_cost_millions",
        "people_cost_concept",
        "validation_status"
    )
    .orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Cálculo dos indicadores de People Economics
# MAGIC
# MAGIC Com os dados financeiros, headcount e People Cost integrados, serão calculados os principais indicadores de People Economics.
# MAGIC
# MAGIC Revenue per Employee e Operating Income per Employee serão calculados para as cinco empresas.
# MAGIC
# MAGIC People Cost / Revenue e People ROI serão calculados somente para as empresas com People Cost validado.

# COMMAND ----------

from pyspark.sql.functions import col, round, when

df_people_economics_final = (
    df_people_economics

    .withColumn(
        "revenue_per_employee",
        round(
            col("revenue").cast("double") /
            col("headcount").cast("double"),
            2
        )
    )

    .withColumn(
        "operating_income_per_employee",
        round(
            col("operating_income").cast("double") /
            col("headcount").cast("double"),
            2
        )
    )

    .withColumn(
        "people_cost_revenue",
        when(
            col("people_cost_millions").isNotNull(),
            round(
                col("people_cost_millions").cast("double") /
                (col("revenue").cast("double") / 1_000_000),
                4
            )
        )
    )

    .withColumn(
        "people_roi",
        when(
            col("people_cost_millions").isNotNull(),
            round(
                (col("operating_income").cast("double") / 1_000_000) /
                col("people_cost_millions").cast("double"),
                4
            )
        )
    )
)

display(
    df_people_economics_final
    .select(
        "name",
        "fy",
        "headcount",
        "revenue_per_employee",
        "operating_income_per_employee",
        "people_cost_millions",
        "people_cost_revenue",
        "people_roi"
    )
    .orderBy("name")
)

# COMMAND ----------

df_people_economics_gold = (
    df_people_economics_final
    .select(
        "name",
        "fy",
        "headcount",
        "revenue",
        "operating_income",
        "revenue_per_employee",
        "operating_income_per_employee",
        "people_cost_millions",
        "people_cost_revenue",
        "people_roi",
        "people_cost_concept",
        "validation_status"
    )
)

display(
    df_people_economics_gold
    .orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Validação dos indicadores
# MAGIC
# MAGIC Antes da análise dos resultados, os indicadores serão submetidos a validações matemáticas e de consistência.
# MAGIC
# MAGIC Serão verificadas:
# MAGIC - presença das cinco empresas;
# MAGIC - coerência entre Revenue, Operating Income e Headcount;
# MAGIC - cálculo de Revenue per Employee;
# MAGIC - cálculo de Operating Income per Employee;
# MAGIC - cálculo do People Cost / Revenue quando disponível;
# MAGIC - cálculo do People ROI somente para People Cost validado.

# COMMAND ----------

df_validation = (
    df_people_economics_gold
    .select(
        "name",
        "fy",
        "revenue",
        "operating_income",
        "headcount",
        "revenue_per_employee",
        "operating_income_per_employee",
        "people_cost_millions",
        "people_cost_revenue",
        "people_roi",
        "validation_status"
    )
)

display(
    df_validation
    .orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Análise dos indicadores de produtividade
# MAGIC
# MAGIC Revenue per Employee e Operating Income per Employee permitem observar a relação entre o desempenho econômico e o tamanho da força de trabalho.
# MAGIC
# MAGIC Revenue per Employee representa a receita associada a cada empregado.
# MAGIC
# MAGIC Operating Income per Employee representa o lucro operacional associado a cada empregado.
# MAGIC
# MAGIC As diferenças entre as empresas devem ser analisadas considerando seus diferentes modelos de negócio, composição da força de trabalho e estruturas operacionais.

# COMMAND ----------

from pyspark.sql.functions import col, round

df_productivity_analysis = (
    df_people_economics_gold
    .select(
        "name",
        "fy",
        "headcount",
        "revenue_per_employee",
        "operating_income_per_employee"
    )
    .withColumn(
        "operating_margin",
        round(
            col("operating_income_per_employee") /
            col("revenue_per_employee"),
            4
        )
    )
)

display(
    df_productivity_analysis
    .orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Hipóteses para explicar as diferenças
# MAGIC
# MAGIC As diferenças observadas nos indicadores não devem ser interpretadas isoladamente como diferenças de produtividade individual.
# MAGIC
# MAGIC O desempenho econômico por empregado pode ser influenciado por características do modelo de negócio, estrutura de custos, composição da força de trabalho, intensidade operacional, terceirização e margem operacional.
# MAGIC
# MAGIC A análise será conduzida a partir de hipóteses que deverão ser confrontadas com evidências antes de qualquer interpretação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Hipóteses para explicar as diferenças
# MAGIC
# MAGIC As diferenças observadas nos indicadores serão analisadas por meio de hipóteses relacionadas ao modelo de negócio, estrutura operacional, margem e intensidade de mão de obra.
# MAGIC
# MAGIC | Hipótese | Indicador relacionado | Evidência a investigar |
# MAGIC |---|---|---|
# MAGIC | O modelo de negócio influencia os indicadores por empregado | Revenue per Employee; Operating Income per Employee | Composição e estrutura da força de trabalho |
# MAGIC | A margem operacional influencia o resultado econômico por empregado | Operating Income per Employee | Relação entre Revenue per Employee e Operating Margin |
# MAGIC | A intensidade de mão de obra influencia o peso do People Cost | People Cost / Revenue | Estrutura e composição dos custos de pessoal |
# MAGIC | A estrutura de custos de pessoal influencia a relação entre investimento em pessoas e resultado operacional | People ROI | People Cost, Operating Income e conceito contábil utilizado |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Análise do People ROI
# MAGIC
# MAGIC O People ROI relaciona o Operating Income ao People Cost validado.
# MAGIC
# MAGIC People ROI = Operating Income / People Cost
# MAGIC
# MAGIC Nesta etapa, serão analisadas somente as empresas com People Cost validado, preservando a comparabilidade dos dados.

# COMMAND ----------

df_people_roi = (
    df_people_economics_gold
    .filter(col("people_cost_millions").isNotNull())
    .select(
        "name",
        "fy",
        "revenue",
        "operating_income",
        "people_cost_millions",
        "people_cost_revenue",
        "people_roi",
        "people_cost_concept"
    )
)

display(
    df_people_roi.orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6.1 People Cost per Employee
# MAGIC
# MAGIC O People Cost per Employee relaciona o custo de pessoas ao tamanho da força de trabalho.
# MAGIC
# MAGIC People Cost per Employee = People Cost / Headcount
# MAGIC
# MAGIC O indicador será utilizado como parâmetro interno para analisar a relação entre o custo de pessoas e o número de empregados, sem estabelecer, isoladamente, um nível ótimo de remuneração.

# COMMAND ----------

from pyspark.sql.functions import col, round

df_people_roi = (
    df_people_economics_gold
    .filter(col("people_cost_millions").isNotNull())
    .withColumn(
        "people_cost_per_employee",
        round(
            (col("people_cost_millions") * 1_000_000) /
            col("headcount"),
            2
        )
    )
)

display(
    df_people_roi.select(
        "name",
        "fy",
        "headcount",
        "people_cost_millions",
        "people_cost_per_employee",
        "people_cost_revenue",
        "people_roi"
    ).orderBy("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Análise dos indicadores
# MAGIC
# MAGIC Os indicadores mostram que tamanho da força de trabalho, geração de receita e resultado operacional apresentam relações diferentes entre as empresas.
# MAGIC
# MAGIC Revenue per Employee e Operating Income per Employee permitem observar a produtividade econômica associada ao headcount, enquanto People Cost per Employee e People Cost / Revenue permitem analisar a dimensão do investimento em pessoas.
# MAGIC
# MAGIC O People ROI complementa essa análise ao relacionar People Cost ao Operating Income. Entretanto, sua interpretação deve considerar o modelo de negócio e as diferenças de classificação contábil do People Cost.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6.1 Síntese dos indicadores de People Economics
# MAGIC
# MAGIC | Indicador | Starbucks | UPS |
# MAGIC |---|---:|---:|
# MAGIC | Revenue per Employee | US$ 97.596,85 | US$ 192.741,30 |
# MAGIC | Operating Income per Employee | US$ 7.707,61 | US$ 17.102,17 |
# MAGIC | People Cost per Employee | US$ 23.172,18 | US$ 105.663,04 |
# MAGIC | People Cost / Revenue | 23,74% | 54,82% |
# MAGIC | People ROI | 0,33x | 0,16x |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6.2 Insight inicial
# MAGIC
# MAGIC Os indicadores mostram diferentes relações entre força de trabalho, geração de receita, custo de pessoas e resultado operacional.
# MAGIC
# MAGIC O People Cost per Employee e o People Cost / Revenue ajudam a dimensionar o investimento em pessoas, enquanto o People ROI relaciona esse investimento ao resultado operacional.
# MAGIC
# MAGIC Esses indicadores, isoladamente, não determinam o nível ótimo de remuneração e benefícios. Para apoiar uma decisão, será necessário avaliar o impacto incremental de diferentes níveis de investimento em pessoas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Cenários de investimento
# MAGIC
# MAGIC Para avaliar o impacto potencial de novos investimentos em pessoas, serão simulados aumentos de 5% e 10% no People Cost.
# MAGIC
# MAGIC O objetivo é calcular quanto de Operating Income adicional seria necessário para manter o People ROI atual.
# MAGIC
# MAGIC Os cenários são análises de sensibilidade e não representam previsão ou relação causal.

# COMMAND ----------

from pyspark.sql.functions import col, lit, round

df_scenarios = (
    df_people_roi
    .select(
        "name",
        "people_cost_millions",
        "operating_income",
        "people_roi"
    )
    .crossJoin(
        spark.createDataFrame(
            [(0.05,), (0.10,)],
            ["investment_increase"]
        )
    )
    .withColumn(
        "additional_people_cost",
        round(
            col("people_cost_millions") * col("investment_increase"),
            2
        )
    )
    .withColumn(
        "required_additional_operating_income",
        round(
            col("additional_people_cost") * col("people_roi"),
            2
        )
    )
)

display(
    df_scenarios
    .select(
        "name",
        "investment_increase",
        "additional_people_cost",
        "required_additional_operating_income"
    )
    .orderBy("name", "investment_increase")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 Impacto na produtividade
# MAGIC
# MAGIC Os cenários anteriores calcularam o Operating Income adicional necessário para sustentar um aumento no People Cost.
# MAGIC
# MAGIC Nesta etapa, esse valor será convertido em aumento necessário de Operating Income per Employee, mantendo o Headcount atual.
# MAGIC
# MAGIC O objetivo é identificar a produtividade econômica adicional necessária para sustentar cada cenário.

# COMMAND ----------

from pyspark.sql.functions import col, round

df_productivity_scenarios = (
    df_scenarios
    .join(
        df_people_roi.select(
            "name",
            "headcount",
            "operating_income_per_employee"
        ),
        on="name",
        how="left"
    )
    .withColumn(
        "required_operating_income_per_employee_increase",
        round(
            (col("required_additional_operating_income") * 1_000_000) /
            col("headcount"),
            2
        )
    )
    .withColumn(
        "required_operating_income_per_employee",
        round(
            col("operating_income_per_employee") +
            col("required_operating_income_per_employee_increase"),
            2
        )
    )
)

display(
    df_productivity_scenarios.select(
        "name",
        "investment_increase",
        "operating_income_per_employee",
        "required_operating_income_per_employee_increase",
        "required_operating_income_per_employee"
    ).orderBy("name", "investment_increase")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9 Conclusão executiva
# MAGIC
# MAGIC A análise mostra que indicadores de People Economics devem ser avaliados em conjunto.
# MAGIC
# MAGIC Revenue per Employee e Operating Income per Employee permitem observar a relação entre força de trabalho e desempenho econômico. People Cost per Employee e People Cost / Revenue dimensionam o investimento em pessoas.
# MAGIC
# MAGIC O People ROI complementa essa visão, mas não determina isoladamente o nível ótimo de remuneração e benefícios.
# MAGIC
# MAGIC Os cenários demonstram que um aumento no People Cost exige um resultado operacional adicional para manter o retorno atual. Esse resultado pode ser traduzido em uma meta de Operating Income per Employee, criando um parâmetro econômico para avaliar diferentes níveis de investimento em pessoas.
# MAGIC
# MAGIC Dessa forma, People Economics pode apoiar decisões de remuneração e benefícios por meio da análise de cenários, metas de produtividade e retorno econômico incremental.