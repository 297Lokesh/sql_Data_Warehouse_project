Data Warehouse Project using Medallion Architecture (MySQL)

Overview
--------
This project implements a Data Warehouse using the Medallion Architecture 
(Bronze, Silver, Gold) in MySQL. The objective is to transform raw CSV data 
into clean, structured, and analytics-ready datasets through an end-to-end 
ETL (Extract, Transform, Load) pipeline.

The project demonstrates how raw data can be processed step-by-step into 
meaningful insights using proper data modeling and transformation techniques.


Data Source
-----------
Source Type:
CSV Files

Domains:
- CRM (Customer, Product, Sales data)
- ERP (Customer details, Location, Product Category)

Data Issues in Source:
- Missing (NULL) values
- Invalid formats (dates, numbers)
- Duplicate records
- Inconsistent text values (e.g., gender, status)

These issues are handled in later layers.


Architecture: Medallion Model
-----------------------------
CSV Files -> Bronze Layer -> Silver Layer -> Gold Layer


Bronze Layer (Raw Data Layer)
----------------------------
Purpose:
The Bronze layer stores raw data exactly as it comes from the source.

Implementation:
- Data is loaded directly from CSV files into MySQL tables
- All columns are stored mostly as VARCHAR
- No transformations or cleaning is performed

Key Features:
- Preserves original data
- Acts as backup layer
- Helps in reprocessing data if needed
- Ensures no data loss


Silver Layer (Cleaned Data Layer)
--------------------------------
Purpose:
The Silver layer is responsible for cleaning and transforming data.

Transformations Performed:
- Data type conversion (VARCHAR -> INT, DATE, etc.)
- Handling invalid values by converting them to NULL
- Removing duplicate records using ROW_NUMBER()
- Trimming unwanted spaces in text fields
- Standardizing categorical values:
  - Gender -> Male / Female / N/A
  - Marital Status -> Single / Married / N/A
- Validating numeric and date formats using REGEXP

Key Features:
- Produces clean and consistent data
- Improves data quality
- Prepares data for business logic
- Ensures reliability for analysis


Gold Layer (Business / Analytics Layer)
--------------------------------------
Purpose:
The Gold layer provides data in a format suitable for analytics and reporting.

Data Model:
Star Schema

Tables Created:
- Dimension Tables:
  dim_customers
  dim_products

- Fact Table:
  fact_sales

Implementation:
- Created using SQL Views
- Used ROW_NUMBER() to generate surrogate keys
- Joined multiple Silver tables to enrich data
- Combined CRM and ERP data for better insights

Key Features:
- Optimized for reporting and BI tools
- Simplifies complex queries
- Provides business-ready datasets


ETL Process
-----------
1. Extract:
   Load data from CSV files into Bronze tables

2. Transform:
   Clean and validate data in Silver layer

3. Load:
   Create Gold views for analytics


Technologies Used
-----------------
- MySQL (Database)
- SQL (Queries, transformations)
- CSV files (Data source)


Key Concepts Applied
--------------------
- Medallion Architecture
- ETL Pipeline
- Data Cleaning and Validation
- Window Functions (ROW_NUMBER)
- Star Schema Design
- Surrogate Keys
- Data Standardization


Project Highlights
------------------
- Built complete end-to-end data pipeline
- Handled real-world data issues
- Designed scalable data warehouse structure
- Created analytics-ready data model
- Integrated data from multiple sources (CRM + ERP)


Future Enhancements
-------------------
- Add data quality checks and monitoring
- Automate ETL using stored procedures
- Implement incremental data loading
- Connect with BI tools like Power BI or Tableau


Conclusion
----------
This project demonstrates how raw data can be transformed into meaningful 
insights using a structured data warehouse approach. It showcases practical 
implementation of ETL processes, data cleaning, and dimensional modeling 
using MySQL.
