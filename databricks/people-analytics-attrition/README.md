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
