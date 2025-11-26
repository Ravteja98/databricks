# NYC Taxi Databricks Lakehouse Project

This project implements an end-to-end **Medallion Architecture** on Databricks Community Edition using the NYC Yellow Taxi dataset.

The pipeline follows a full production-style design:
- Bronze: Raw ingestion + audit columns
- Silver: Cleaned, business-ready dataset with data quality rules
- Gold: Star schema (Dimensions + Fact) for analytics and reporting

## Architecture

**RAW → BRONZE → SILVER → GOLD (Star Schema)**

Gold layer includes:
- dim_vendor
- dim_payment
- dim_location
- dim_rate
- dim_time
- fact_trips (~18.2M records)

## Technologies
- Databricks Community Edition
- PySpark
- Delta Lake
- GitHub
- Dimensional Modeling (Star Schema)
- Data Warehouse concepts

## Key Features
- Added audit columns: `_source_file`, `ingested_at`
- Implemented business validation rules in Silver
- Designed natural key-based dimensions
- Built a lean, analytics-ready fact table
- Structured for KPI dashboards (peak hours, revenue by borough, top zones)

## Folder Structure

