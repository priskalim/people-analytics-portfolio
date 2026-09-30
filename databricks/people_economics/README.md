# People Economics — Databricks

Análise de People Economics utilizando dados públicos da SEC 10-K para investigar a relação entre força de trabalho, People Cost e desempenho econômico.

**5 empresas | 19.014 registros | Databricks Free Edition | SEC 10-K**

---

## Objetivo

Investigar como indicadores de workforce e dados financeiros podem ser combinados para apoiar decisões sobre investimento em pessoas.

A análise busca transformar dados financeiros e de workforce em indicadores econômicos e cenários que possam apoiar decisões de Workforce Planning e People Analytics.

---

## Empresas analisadas

- Starbucks
- UPS
- Costco
- Microsoft
- Walmart

---

## Dados

Os dados foram obtidos a partir dos filings **10-K disponibilizados publicamente pela SEC**.

A exploração inicial utilizou os datasets:

- `SUB`
- `PRE`
- `NUM`

A base `SUB` utilizada na análise continha **19.014 registros**.

---

## Fluxo da análise

```text
SEC 10-K
   ↓
Exploração dos dados
   ↓
Validação dos dados financeiros
   ↓
Integração com Headcount
   ↓
Investigação do People Cost
   ↓
Construção dos indicadores
   ↓
Análise de cenários
   ↓
Insights para decisão
