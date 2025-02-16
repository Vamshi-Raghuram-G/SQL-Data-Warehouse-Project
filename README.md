# SQL-Data-Warehouse-Project

#### This project demonstrates a comprehensive data warehousing and analytics solution, covering everything from building a data warehouse to generating actionable insights.

## Data Architecture
#### The data architecture for this project follows the Medallion Architecture, comprising Bronze, Silver, and Gold layers:

1. Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV files into a SQL Server database.  
2. Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare the data for analysis.  
3. Gold Layer: Houses business-ready data, modeled into a star schema, required for reporting and analytics.
