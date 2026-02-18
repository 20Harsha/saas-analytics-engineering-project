# End-to-End SaaS Analytics Data Pipeline

A complete data engineering project that simulates a real-world SaaS analytics platform using:

- AWS RDS (Postgres) as the transactional database
- AWS S3 for event and payment data
- Snowflake as the cloud data warehouse
- Python for data ingestion
- dbt for transformation and modeling
- Power BI for business dashboards

This project demonstrates modern data stack architecture, incremental loading, SCD snapshots, dbt best practices and analytics-ready data modeling.

## ⚙️ Architecture Overview

https://github.com/user-attachments/assets/e8a1ede9-a1f2-4d5f-a53d-73c8db8cf7d0

### 💡High-Level Flow

1. Source data comes from:
   - AWS RDS (Postgres): users, plans, subscriptions
   - AWS S3: events and payments

2. Python ingestion scripts load data incrementally into Snowflake raw tables:
   - Incremental extraction from RDS tables
   - S3 files loaded via Snowflake external stage

3. dbt transforms raw data into:
   - Staging layer
   - Intermediate layer
   - Mart layer

4. Power BI connects to the mart layer for analytics dashboards.

5. GitHub is used for version control.

## Data Sources Overview

1. Source Tables (RDS)
   - users
   - plans
   - subscriptions

2. S3 Files
   - events.csv
   - payments.json

## <img width="20" height="20" alt="snowflake-bug-color-rgb@2x" src="https://github.com/user-attachments/assets/a3d498f8-8a40-48c6-ab83-d5bc2566d00b" /> Snowflake Layers

1. External Stage
Used for S3 data integration.

2. Raw Layer
Direct ingestion from sources:
- users_raw
- plans_raw
- subscriptions_raw
- events_raw
- payments_raw

3. Staging Layer (dbt)
Cleaned, standardized tables:
- stg_users
- stg_plans
- stg_subscriptions
- stg_events
- stg_payments

4. Intermediate Layer (dbt)
Used for modular transformations and joins:
- int_subscription_details

5. Mart Layer (dbt)
Business-ready models:
- fct_subscriptions
- fct_payments
- fct_events
- dim_users
- dim_plans

## Data Model
The analytics layer follows a star schema with subscription, payment, and event facts connected to user and plan dimensions.

<img width="700" height="666" alt="data model" src="https://github.com/user-attachments/assets/1ef5c8e3-fa71-410d-9df8-ebbf296cedba" />


## Key Engineering Concepts Implemented

### 1. **Incremental Data Loading**
   - Implemented incremental extraction in Python using the `updated_at` column.
   - Only new or changed records are processed, avoiding full table reloads.
   - Raw tables use upsert logic to:
     - Maintain one row per business key
     - Prevent duplicates
     - Ensure the latest record overwrites previous values

  Also implemented in dbt staging models using:
   - materialized = incremental
   - incremental_strategy = merge

### 2. **SCD Type 2 Snapshots (dbt)**
Implemented for the users table:
Tracks changes in:
  - email
  - country
  - acquisition_channel
  - company_size

Maintains historical records for analysis

### 3. **Data Quality and Best Practices**
- Implemented dbt tests for data quality:
  - Not null tests
  - Unique key validation
  - Referential integrity checks

- Macros used to:
  - Mask PII data (email, phone)
  - Exclude test records
 

## <img width="30" height="30" alt="Power-BI" src="https://github.com/user-attachments/assets/ce1aa523-bd1e-478d-ba13-59fbddbd12d9" /> Analytics Dashboard:
The Power BI report includes four pages.

#### 1. **Revenue Overview**

<img width="600" height="600" alt="01_retention_overview" src="https://github.com/user-attachments/assets/3ba29bb4-810a-4e51-8634-e984bb6648cb" />


#### 2. **Growth Metrics**

<img width="600" height="600" alt="02_growth_metrics" src="https://github.com/user-attachments/assets/f54fb100-d355-4c93-9b8f-4286da1a6939" />


#### 3. **Retention & Churn**

<img width="600" height="600" alt="03_retention_and_churn" src="https://github.com/user-attachments/assets/31df48c4-957b-4ac1-a648-6fb22bc9d5f4" />


#### 4. **Product Engagement**

<img width="600" height="600" alt="04_product_engagement" src="https://github.com/user-attachments/assets/51a957ba-758c-4530-aab1-5d1a248e0d00" />

