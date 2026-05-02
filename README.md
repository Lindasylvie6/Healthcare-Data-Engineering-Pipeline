# 🏥 Healthcare Data Engineering & Analytics Platform
### Enterprise Case Study: Modernizing Healthcare KPI Reporting with Azure & Databricks
---
### 1. Executive Summary

Healthcare organizations often struggle with fragmented data sources, inconsistent KPI definitions, and delayed reporting for operational and clinical decision-making.

This project simulates an enterprise-grade data platform designed to unify healthcare data and deliver trusted, near-real-time executive insights using a modern lakehouse architecture.

The solution centralizes patient, encounter, provider, and claims data into a governed pipeline and delivers an Executive Summary dashboard used for high-level performance monitoring.

---
### 2. Business Problem

Healthcare stakeholders faced the following challenges:

- KPI inconsistencies (e.g., readmission rate definition varied across reports)
- Delayed reporting cycles for executive dashboards
- Fragmented datasets across operational systems
- Limited visibility into provider and clinical performance trends
- Business Objective

#### Build a unified analytics platform that:

- Standardizes healthcare KPIs
- Improves data reliability and traceability
- Enables faster executive decision-making
---
### 3. Solution Overview

A modern medallion architecture (Bronze → Silver → Gold) was implemented using Azure Data Factory and Databricks.

##### Architecture Flow
- Azure Data Factory → Data ingestion from raw healthcare sources
- Databricks (Bronze Layer) → Raw ingestion and initial storage
- Databricks (Silver Layer) → Data cleaning, validation, standardization
- Databricks (Gold Layer) → KPI aggregation and business-ready datasets
- Power BI → Executive dashboards and reporting layer

--- 
### 5. Executive Summary Dashboard (Power BI)

The final dashboard was designed for executive-level stakeholders to monitor healthcare performance across key dimensions:

- Readmission rates
- Encounter volumes
- Provider performance
- Revenue and utilization trends

##### The dashboard enables:

- Faster identification of operational inefficiencies
- Standardized KPI interpretation across departments
- High-level visibility into healthcare system performance

---  

### 6. Databricks Implementation (Gold Layer Focus)
- Developed PySpark-based transformations for KPI aggregation
- Designed and optimized the fact_monthly_kpis gold table
- Used Delta Lake for reliability, versioning, and performance optimization
- Integrated Databricks SQL endpoints for BI consumption
- Leveraged AI-assisted tools (Databricks Genie) to accelerate exploration, validate logic, and refine KPI definitions during development
  
---
### 7. Key Design Decisions
- Adopted medallion architecture to enforce data quality progression
- Centralized KPI logic in the gold layer to avoid metric fragmentation
- Used Delta Lake for ACID compliance and historical traceability
- Separated transformation logic (Databricks) from visualization layer (Power BI)

--- 
### 8. Outcome / Impact
This architecture demonstrates:
- A scalable approach to healthcare analytics
- Improved consistency of KPI definitions
- Reduced complexity in downstream reporting
- A reusable framework for enterprise-grade data pipelines

---
### 9. Technologies Used
- Azure Data Factory
- Azure Data Lake
- Databricks (PySpark, Delta Lake, SQL)
- Power BI
- AI-assisted development (Databricks Genie)

---

### License

This project is licensed under the [MIT LICENSE](LICENSE). You are free to use, modify, and share this project with proper attribution.

---
### 👤 About me

Hi there! I'm **Sylvie Linda**. I am a data professional with experience in data analysis and a strong interest in data engineering. I enjoy working with data pipelines, transforming raw data into meaningful insights, and improving data quality and efficiency.

This project reflects my focus on building scalable data solutions and applying best practices in data engineering, including ETL processes, data modeling, and pipeline design.
