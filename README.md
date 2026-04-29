# Healthcare Data Warehouse and Analytics Project
---
### 🚀 Overview

This project demonstrates the design of an end-to-end healthcare data pipeline using Azure Data Factory concepts and a **medallion architecture** (Bronze, Silver, Gold). It incorporates **ETL processes**, **SQL**-based data transformation, and data modeling to ingest, clean, and standardize raw datasets into analytics-ready data.

The project emphasizes **data integration, data quality validation, and troubleshooting**, reflecting core data engineering responsibilities such as **building, optimizing, and maintaining scalable data pipelines**. 

This project simulates a real-world data engineering workflow from ingestion to analytics.

---

### 🧱 Data Architecture (Medallion Model)
The pipeline is orchestrated using Azure Data Factory, which manages ingestion, transformation, and data movement across layers.
<img width="716" height="535" alt="DWH_Architecture" src="https://github.com/user-attachments/assets/dbec3efd-2558-4b25-876f-2e091290f961" />

#### 🟤 Bronze Layer (Raw Data)
- Stores raw CSV files as-is
- Minimal transformation
- Batch ingestion
  
#### 🥈 Silver Layer (Cleaned Data)
- Data cleaning and standardization
- Deduplication and validation
- Ensures data quality
  
#### 🥇 Gold Layer (Business-Ready Data)
- Business logic and aggregations
- Star schema (fact and dimension tables)
- Optimized for reporting

#### 📊 Consume Layer
- Power BI dashboards
- Reports and analytics
- End-user data access

<img width="1440" height="900" alt="Screenshot 2026-04-28 at 4 55 50 PM" src="https://github.com/user-attachments/assets/147bb1d6-3f66-4681-b6d4-c18699c2dee4" />


---
### ⚙️ Data Pipeline (ADF)
- Pipeline orchestration using Azure Data Factory
- Copy Activity for ingestion
- Data Flows / SQL for transformations
- Scheduling and monitoring
- Error handling and logging
  
<img width="1440" height="807" alt="pl_Marter_bronze_load" src="https://github.com/user-attachments/assets/fd47ec4a-b54b-465a-b2d4-670043b81cfa" />

---
### 🧪 Data Quality & Validation
- Removed duplicate records
- Handled missing values
- Validated primary and foreign keys
- Standardized formats (dates, text fields)
- Ensured referential integrity

---
### 🚨 Data Quality Issue & Resolution

During dashboard development, the readmission rate appeared as **52%**, which was unrealistic.

##### Root Cause:
Outpatient and telehealth visits were incorrectly flagged as readmissions.

##### Solution:
- Analyzed encounter data at the Silver layer
- Filtered readmission logic to only include inpatient visits
- Validated results using SQL aggregation

##### Result:
Corrected readmission rate to **19.8%**, aligning with clinical expectations.

---
### 📊 Example Use Cases
- Patient visit analysis
- Provider performance tracking
- Claims and billing insights
- Denial rate analysis

---
### 🛠️ Tools & Technologies

- **Azure Data Factory (Concepts)** : Pipeline orchestration, data ingestion, and transformation  
- **SQL** : Data transformation, validation, and modeling  
- **Power BI** : Data visualization and reporting  
- **Draw.io** : Data architecture and pipeline diagrams  
- **Notion** : Project planning, task tracking, and documentation  

---
### License

This project is licensed under the [MIT LICENSE](LICENSE). You are free to use, modify, and share this project with proper attribution.

---
### 👤 About me

Hi there! I'm **Sylvie Linda**. I am a data professional with experience in data analysis and a strong interest in data engineering. I enjoy working with data pipelines, transforming raw data into meaningful insights, and improving data quality and efficiency.

This project reflects my focus on building scalable data solutions and applying best practices in data engineering, including ETL processes, data modeling, and pipeline design.
