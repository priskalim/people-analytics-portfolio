# People Analytics — Employee Attrition

## Overview

End-to-end People Analytics project developed in Databricks to transform employee data into attrition indicators and business insights.

The project demonstrates the data journey from raw HR data to an analytical dashboard using the Medallion Architecture.

> **Note:** The IBM HR Analytics dataset is a fictional/educational dataset. The Attrition Rate shown in this project represents the proportion of records with `Attrition = Yes`; it is not a turnover rate calculated over a time period.

---

## Business Question

**Which employee characteristics are associated with higher observed attrition in the dataset?**

The analysis explores attrition across:

- Overtime
- Business Travel
- Department
- Job Role

---

## Architecture

```text
IBM HR Analytics
       │
       ▼
   ┌─────────┐
   │ BRONZE  │
   │ Raw data│
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │ SILVER  │
   │ Quality │
   │ & Clean │
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │  GOLD   │
   │ HR KPIs │
   └────┬────┘
        │
        ▼
   ┌─────────────┐
   │  Dashboard  │
   │  Attrition  │
   └─────────────┘

```
---

## Data

**Dataset:** IBM HR Analytics Employee Attrition & Performance

**Population analyzed:** 1,470 employees

**Observed attritions:** 237

**Observed attrition rate:** 16.12%

---

## Data Pipeline

### 1. Bronze — Raw Data

The source data is ingested into Databricks with the objective of preserving the original information before analytical transformations.

Main activities:

- File ingestion
- Schema inspection
- Initial record validation
- Raw data storage

### 2. Silver — Data Quality & Transformation

The Silver layer prepares the data for analysis.

Main activities:

- Data type validation
- Duplicate checks
- Null-value checks
- Standardization of categorical fields
- Validation of business rules
- Preparation of analytical attributes

### 3. Gold — People Analytics

The Gold layer contains business-oriented metrics used by the dashboard.

Examples:

- Employee count
- Attrition count
- Attrition rate
- Attrition by overtime
- Attrition by business travel
- Attrition by department
- Attrition by job role

---

## Dashboard

![People Analytics — Attrition](dashboard_attrition.png)

### Main indicators

| Indicator | Result |
|---|---:|
| Employees | 1,470 |
| Attritions | 237 |
| Attrition Rate | 16.12% |

### Analytical dimensions

- **Overtime**
- **Business Travel**
- **Department**
- **Job Role**

---

## Key Analytical Observations

The dashboard shows differences in observed attrition across the analyzed employee groups.

For example, the observed attrition proportion is higher among employees classified with overtime than among those without overtime.

This represents an **association in the dataset**, not evidence that overtime causes attrition.

The same approach is applied to business travel, department and job role: the dashboard identifies patterns that can be investigated further rather than establishing causality.

---

## Technical Stack

- **Databricks**
- **Apache Spark / PySpark**
- **SQL**
- **Delta Lake**
- **Medallion Architecture**
- **People Analytics**
- **Data Quality**

---

## Skills Demonstrated

### Data

- Data ingestion
- Data profiling
- Data quality validation
- Data transformation
- Aggregation
- Analytical data modeling

### Databricks

- Databricks notebooks
- SQL
- PySpark
- DataFrames
- Delta-based data layers
- Medallion Architecture
- Dashboard creation

### People Analytics

- Attrition analysis
- HR KPI definition
- Workforce segmentation
- Business-oriented data interpretation
- Distinction between association and causality

---

## Next Steps

Potential extensions of the analysis:

- Attrition by age group
- Attrition by salary band
- Attrition by years at company
- Attrition by job level
- Employee satisfaction analysis
- Multivariate analysis
- Predictive attrition modeling

---

## Author

**Priscila Lima**

People Analytics | Workforce Planning | HR Data Analytics
